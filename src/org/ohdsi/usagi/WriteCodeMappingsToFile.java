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
package org.ohdsi.usagi;

import java.util.ArrayList;
import java.util.List;

import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteCSVFileWithHeader;

/**
 * Class for writing code mappings (source codes and mapped target concept(s)) to a CSV file.
 */
public class WriteCodeMappingsToFile {
	private WriteCSVFileWithHeader out;

	public WriteCodeMappingsToFile(String filename) {
		out = new WriteCSVFileWithHeader(filename);
	}

	public void write(CodeMapping codeMapping) {
		List<Concept> targetConcepts;
		if (codeMapping.targetConcepts.size() == 0) {
			targetConcepts = new ArrayList<Concept>(1);
			targetConcepts.add(Concept.EMPTY_CONCEPT);
		} else
			targetConcepts = codeMapping.targetConcepts;
		for (Concept targetConcept : targetConcepts) {
			Row row = codeMapping.sourceCode.toRow();
			row.add("matchScore", codeMapping.matchScore);
			row.add("mappingStatus", codeMapping.mappingStatus.toString());
			row.add("targetConceptId", targetConcept.conceptId);
			row.add("targetConceptName", targetConcept.conceptName);
			row.add("targetVocabularyId", targetConcept.vocabularyId);
			row.add("targetDomainId", targetConcept.domainId);
			row.add("targetStandardConcept", targetConcept.standardConcept);
			row.add("targetChildCount", targetConcept.childCount);
			row.add("targetParentCount", targetConcept.parentCount);
			row.add("targetConceptClassId", targetConcept.conceptClassId);
			row.add("targetConceptCode", targetConcept.conceptCode);
			row.add("targetValidStartDate", targetConcept.validStartDate);
			row.add("targetValidEndDate", targetConcept.validEndDate);
			row.add("targetInvalidReason", targetConcept.invalidReason);
			out.write(row);
		}
	}

	public void close() {
		out.close();
	}
}
