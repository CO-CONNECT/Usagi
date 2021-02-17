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



    private void createInitialMappingThreads(List<SourceCode> sourceCodes, ImportSettings settings) throws ExecutionException, InterruptedException
    {
        WriteCodeMappingsToFile primary = new WriteCodeMappingsToFile(settings.outputDir+"/"+"main_mappings.csv");
        WriteCodeMappingsToFile secondary = new WriteCodeMappingsToFile(settings.outputDir+"/"+"secondary_mappings.csv");

        List<CodeMapping> globalMappingList = Collections.synchronizedList(Global.mapping);
        List<CodeMapping> secondaryMappingList = Collections.synchronizedList(Global.mapping);

        int threadCount = Runtime.getRuntime().availableProcessors();

        if (threadCount <= 0)
            threadCount = 1;

        // Note: Lucene's and BerkeleyDB's search objects are thread safe, so do not need to be recreated for each thread.
        ForkJoinPool forkJoinPool = new ForkJoinPool(threadCount);
        forkJoinPool.submit(() -> sourceCodes.parallelStream().forEach(sourceCode -> {
            try
            {
                String textToMap = sourceCode.sourceName;
                String codeToMap = sourceCode.sourceCode;

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
                boolean empties = (settings.empties.contains(textToMap.toLowerCase()) || settings.empties.contains(codeToMap.toLowerCase()));
                boolean containsNumbers = (StringUtils.isNumeric(textToMap) || (textToMap.startsWith("-") && StringUtils.isNumeric(textToMap.split("-")[1])));
                boolean mapField = ( containsNumbers || positiveStatement || negativeStatement || empties);

                if (!mapField)
                    doMapping(sourceCode, settings, globalMappingList,  sourceCode.sourceName);
                else
                    doMapping(sourceCode, settings, globalMappingList,  sourceCode.fieldDescription);
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
            {
                primary.write(map);
            }

            for (CodeMapping seconadaryConcept: map.otherConcepts)
            {
                if (seconadaryConcept.matchScore >= settings.secondaryThreshold)
                    secondary.write(seconadaryConcept);
            }
        }

          primary.close();
          secondary.close();

    }

    public void doMapping(SourceCode sourceCode, ImportSettings settings, List<CodeMapping> globalMappingList,  String textToUse)
    {
        CodeMapping codeMapping = new CodeMapping(sourceCode);

        List<ScoredConcept> concepts = Global.usagiSearchEngine.search(textToUse, true, sourceCode.sourceAutoAssignedConceptIds,
                settings.filterDomains, settings.filterConceptClasses, settings.filterVocabularies, settings.filterStandard, settings.includeSourceTerms);

        if (concepts.size() > 0)
        {
            int count=0;
            for (ScoredConcept current: concepts)
            {
                if (count==0)
                {
                    codeMapping.targetConcepts.add(current.concept);
                    codeMapping.matchScore = current.matchScore;
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
                }
                else
                {
                    CodeMapping secondaryMap = new CodeMapping(sourceCode);
                    secondaryMap.targetConcepts.add(current.concept);
                    secondaryMap.matchScore = current.matchScore;
                    secondaryMap.mappingStatus = MappingStatus.AUTO_MAPPED;
                    secondaryMap.comment = "";

                    codeMapping.otherConcepts.add(secondaryMap);
                }

                count++;
            }
        }
        else
        {
            codeMapping.matchScore = 0;
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
        public String sourceFile = null;

        /**
         * The full path to where the output files will be written
         */
        public String outputDir = null;

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

        /**
         * THe positive, negative and empties represent the values that may represent a value field, such as 'Yes, Y, 1' or "No, N, 1' If these are preesnt
         * then the usagi will map the column name instead
         */
        public List<String> posTerms = new ArrayList<String>();
        public List<String> negTerms = new ArrayList<String>();
        public List<String> empties = new ArrayList<String>();

        /**
         * Include names of source concepts that map to standard concepts in the search?
         */
        public boolean includeSourceTerms = true;

        /**
         * Threshold to accept a result
         */
        public double threshold = 0.7;

        /**
         * Threshold to accept a secondary result
         */
        public double secondaryThreshold = 0.2;

        /**
         * The ID of the column, usually the column name
         */
        public String fieldID;

        /**
         * The description of the column name
         */

        public String fieldDesc;

    }

}
