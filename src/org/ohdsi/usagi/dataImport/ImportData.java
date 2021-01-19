/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.usagi.dataImport;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.StringUtils;
import org.apache.xmlbeans.impl.xb.ltgfmt.Code;
import org.ohdsi.usagi.CodeMapping;
import org.ohdsi.usagi.CodeMapping.MappingStatus;
import org.ohdsi.usagi.SourceCode;
import org.ohdsi.usagi.Concept;
import org.ohdsi.usagi.UsagiSearchEngine;
import org.ohdsi.usagi.UsagiSearchEngine.ScoredConcept;
import org.ohdsi.usagi.WriteCodeMappingsToFile;
import org.ohdsi.usagi.ui.Global;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.ReadCSVFileWithHeader;
import org.ohdsi.utilities.files.Row;

/**
 * Use this class to programmatically import data into the Usagi format
 *
 * @author MSCHUEMI
 */
public class ImportData
{

    public static String SOURCE_CODE_TYPE_STRING = "S";
    public static String CONCEPT_TYPE_STRING = "C";

    private UsagiSearchEngine usagiSearchEngine;

    public void process(ImportSettings settings) throws ExecutionException, InterruptedException
    {
        usagiSearchEngine = new UsagiSearchEngine(settings.usagiFolder);
        List<SourceCode> sourceCodes = new ArrayList<SourceCode>();

        for (Row row : new ReadCSVFileWithHeader(settings.sourceFile))
            sourceCodes.add(convertToSourceCode(row, settings));

        usagiSearchEngine.createDerivedIndex(sourceCodes, null);

        createInitialMappingThreads(sourceCodes, settings);

    }

    private SourceCode convertToSourceCode(Row row, ImportSettings settings)
    {
        SourceCode sourceCode = new SourceCode();
        sourceCode.sourceCode = row.get(settings.sourceCodeColumn);
        sourceCode.sourceName = row.get(settings.sourceNameColumn);
        sourceCode.fieldID = row.get(settings.fieldID);
        sourceCode.fieldDescription = row.get(settings.fieldDesc);

        if (settings.sourceFrequencyColumn != null)
            sourceCode.sourceFrequency = row.getInt(settings.sourceFrequencyColumn);

        if (settings.autoConceptIdsColumn != null)
        {
            if (!row.get(settings.autoConceptIdsColumn).equals(""))
            {
                for (String conceptId : row.get(settings.autoConceptIdsColumn).split(";"))
                    sourceCode.sourceAutoAssignedConceptIds.add(Integer.parseInt(conceptId));
            }
        }
        for (String additionalInfoColumn : settings.additionalInfoColumns)
            sourceCode.sourceAdditionalInfo.add(new Pair<String, String>(additionalInfoColumn, row.get(additionalInfoColumn)));

        return sourceCode;
    }

    private void createInitialMapping(List<SourceCode> sourceCodes, ImportSettings settings)
    {
        WriteCodeMappingsToFile out = new WriteCodeMappingsToFile(settings.mappingFile);

        for (SourceCode sourceCode : sourceCodes)
        {

            if (StringUtils.isAlphanumeric(sourceCode.sourceName))
            {

                CodeMapping codeMapping = new CodeMapping(sourceCode);

                List<ScoredConcept> concepts = usagiSearchEngine.search(sourceCode.sourceName, true, sourceCode.sourceAutoAssignedConceptIds,
                        settings.filterDomains, settings.filterConceptClasses, settings.filterVocabularies, settings.filterStandard, settings.includeSourceTerms);

                if (concepts.size() > 0)
                {
                    codeMapping.targetConcepts.add(concepts.get(0).concept);
                    codeMapping.matchScore = concepts.get(0).matchScore;
                }
                else
                {
                    codeMapping.targetConcepts.add(Concept.EMPTY_CONCEPT);
                    codeMapping.matchScore = 0;
                }

                codeMapping.mappingStatus = MappingStatus.UNCHECKED;

                if (sourceCode.sourceAutoAssignedConceptIds.size() == 1 && concepts.size() > 0)
                {
                    codeMapping.mappingStatus = MappingStatus.AUTO_MAPPED_TO_1;
                }
                else if (sourceCode.sourceAutoAssignedConceptIds.size() > 1 && concepts.size() > 0)
                {
                    codeMapping.mappingStatus = MappingStatus.AUTO_MAPPED;
                }
                if (codeMapping.matchScore >= settings.threshold)
                    out.write(codeMapping);
            }
        }
        out.close();
    }

    private void createInitialMappingThreads(List<SourceCode> sourceCodes, ImportSettings settings) throws ExecutionException, InterruptedException
    {
        WriteCodeMappingsToFile out = new WriteCodeMappingsToFile(settings.mappingFile);

        List<CodeMapping> globalMappingList = Collections.synchronizedList(Global.mapping);

        int threadCount = Runtime.getRuntime().availableProcessors();
//        int threadCount = 1;

        if (threadCount <= 0)
            threadCount = 1;

        // Note: Lucene's and BerkeleyDB's search objects are thread safe, so do not need to be recreated for each thread.
        ForkJoinPool forkJoinPool = new ForkJoinPool(threadCount);
        forkJoinPool.submit(() -> sourceCodes.parallelStream().forEach(sourceCode -> {




            try
            {
                String textToMap = sourceCode.sourceName;

                SourceCode fieldMap = new SourceCode();
                SourceCode intelMap = new SourceCode();
                copySoureCode(sourceCode, fieldMap);
                copySoureCode(sourceCode, intelMap);

                /**
                 * Decision making flags.
                 * positiveStatement: establishes is the value is a known positive statement (such as Yes)
                 * positiveStatement: establishes is the value is a known negative statement (such as No)
                 * containsNumbers: establishes if the value is a numeric
                 * allAdjs: establishes if all the text values are adjectives (Strong, High, Low)
                 * mapField: if any of the obove is true, then Usagi should map to the fieldDescription not the field entry content
                 */
                boolean positiveStatement = (settings.posTerms.contains(textToMap.toLowerCase()));
                boolean negativeStatement = (settings.negTerms.contains(textToMap.toLowerCase()));
                boolean containsNumbers = (StringUtils.isNumeric(textToMap) || (textToMap.startsWith("-") && StringUtils.isNumeric(textToMap.split("-")[1])));
                boolean mapField = ( containsNumbers || positiveStatement || negativeStatement );

                doMapping(fieldMap, settings, globalMappingList, fieldMap.fieldDescription);

                if (!mapField)
                {
                    doMapping(sourceCode, settings, globalMappingList, sourceCode.sourceName);

//                Set<String> nouns = check.getNoun(fieldMap.sourceName);
//
//                StringBuilder toUse = new StringBuilder(sourceCode.sourceName + " ");
//
//                for (String noun : nouns)
//                {
//                    toUse.append(noun).append(" ");
//                }
//                textToMap = toUse.toString();
//                intelMap.setUsedCheat(true);
//
//                doMapping(intelMap, settings, globalMappingList, textToMap);
                }


            }
            catch (Exception e)
            {
                System.out.println(e.toString());
            }

        })).get();
        forkJoinPool.shutdown();

        for (CodeMapping map : globalMappingList)
        {
            if (map.matchScore >= settings.threshold)
                out.write(map);
        }
    }

    public void doMapping(SourceCode sourceCode, ImportSettings settings, List<CodeMapping> globalMappingList, String textToUse)
    {
        CodeMapping codeMapping = new CodeMapping(sourceCode);

        List<ScoredConcept> concepts = Global.usagiSearchEngine.search(textToUse, true, sourceCode.sourceAutoAssignedConceptIds,
                settings.filterDomains, settings.filterConceptClasses, settings.filterVocabularies, settings.filterStandard, settings.includeSourceTerms);

        if (concepts.size() > 0)
        {
            codeMapping.targetConcepts.add(concepts.get(0).concept);
            codeMapping.matchScore = concepts.get(0).matchScore;
        }
        else
        {
            codeMapping.matchScore = 0;
        }
        codeMapping.comment = "";
        codeMapping.mappingStatus = MappingStatus.UNCHECKED;

        if (sourceCode.sourceAutoAssignedConceptIds.size() == 1 && concepts.size() > 0)
        {
            codeMapping.mappingStatus = MappingStatus.AUTO_MAPPED_TO_1;
        }
        else if (sourceCode.sourceAutoAssignedConceptIds.size() > 1 && concepts.size() > 0)
        {
            codeMapping.mappingStatus = MappingStatus.AUTO_MAPPED;
        }
        synchronized (globalMappingList)
        {
            globalMappingList.add(codeMapping);
        }
    }
    public void copySoureCode(SourceCode toBeCopied, SourceCode copiedTo)
    {
        copiedTo.setFieldDescription(toBeCopied.getFieldDescription());
        copiedTo.setFieldID(toBeCopied.getFieldID());
        copiedTo.setUsedCheat(toBeCopied.isUsedCheat());
        copiedTo.setFieldMapping(toBeCopied.isFieldMapping());
        copiedTo.setSourceCode(toBeCopied.getSourceCode());
        copiedTo.setSourceFrequency(toBeCopied.getSourceFrequency());
        copiedTo.setSourceName(toBeCopied.getSourceName());
        copiedTo.getSourceAdditionalInfo().addAll(toBeCopied.getSourceAdditionalInfo());

    }
    public static class ImportSettings
    {
        /**
         * The root folder of Usagi. This is needed to locate the index
         */
        public String usagiFolder = "";

        /**
         * The root folder for the vocabulary
         */
        public String vocabFolder = "";

        /**
         * The full path to the csv file containing the source code information
         */
        public String sourceFile = "/Users/svzpq/The University of Nottingham/TDCC - ATLAS - ATLAS/ALSPAC/Data Mapping/ALSPAC small copy.csv";

        /**
         * The full path to where the output csv file will be written
         */
        public String mappingFile = "/Users/svzpq/Downloads/Usagi-master/output.csv";

        /**
         * The domain to which the search should be restricted. Set to null if not restricting by domain
         */
        public Vector<String> filterDomains = null;

        /**
         * The concept class to which the search should be restricted. Set to null if not restricting by concept class
         */
        public Vector<String> filterConceptClasses = null;

        /**
         * The vocabulary to which the search should be restricted. Set to null if not restricting by vocabulary
         */
        public Vector<String> filterVocabularies = null;

        /**
         * Specify whether the search should be restricted to standard concepts only. If not, classification concepts will
         * also be allowed.
         */
        public boolean filterStandard = true;

        /**
         * The name of the column containing the source codes
         */
        public String sourceCodeColumn = "";

        /**
         * The name of the column containing the source code names / descriptions
         */
        public String sourceNameColumn = "";

        /**
         * The name of the column containing the source code frequency
         */
        public String sourceFrequencyColumn;

        /**
         * The name of the column containing the automatically assigned concept IDs
         */
        public String autoConceptIdsColumn;

        /**
         * The names of the columns containing additional information about the source codes that should be displayed in Usagi
         */
        public List<String> additionalInfoColumns = new ArrayList<String>();

        public List<String> posTerms = new ArrayList<String>();
        public List<String> negTerms = new ArrayList<String>();

        /**
         * Include names of source concepts that map to standard concepts in the search?
         */
        public boolean includeSourceTerms = true;

        /**
         * Threshold to accept a result
         */
        public double threshold = 0.7;

        public String fieldID;

        public String fieldDesc;

    }

}
