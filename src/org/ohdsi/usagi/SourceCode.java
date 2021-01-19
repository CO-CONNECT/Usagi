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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.Pair;
import org.ohdsi.utilities.files.Row;

/**
 * Data structure for containing information about a source code
 */
public class SourceCode {

	public String						sourceCode;
	public String						sourceName;
	public boolean						usedCheat = false;
	public int							sourceFrequency;
	public Set<Integer>					sourceAutoAssignedConceptIds	= new HashSet<Integer>();
	public List<Pair<String, String>>	sourceAdditionalInfo			= new ArrayList<Pair<String, String>>();
	public String						fieldID;
	public String						fieldDescription;
	public boolean 						fieldMapping=false;

	private static String				ADDITIONAL_INFO_PREFIX			= "ADD_INFO:";

	public Row toRow() {
		Row row = new Row();
		row.add("sourceCode", sourceCode);
		row.add("sourceName", sourceName);
		row.add("sourceFrequency", sourceFrequency);
		row.add("sourceAutoAssignedConceptIds", StringUtilities.join(sourceAutoAssignedConceptIds, ";"));
		for (Pair<String, String> pair : sourceAdditionalInfo) {
			row.add(ADDITIONAL_INFO_PREFIX + pair.getItem1(), pair.getItem2());
		}
		return row;
	}

	public SourceCode() {
	}

	public SourceCode(Row row) {
		sourceCode = row.get("sourceCode");
		sourceName = row.get("sourceName");
		sourceFrequency = row.getInt("sourceFrequency");
		sourceAutoAssignedConceptIds = parse(row.get("sourceAutoAssignedConceptIds"));
		for (String field : row.getFieldNames())
			if (field.startsWith(ADDITIONAL_INFO_PREFIX)) {
				String name = field.substring(ADDITIONAL_INFO_PREFIX.length(), field.length());
				sourceAdditionalInfo.add(new Pair<String, String>(name, row.get(field)));
			}
	}

	private Set<Integer> parse(String string) {
		if (string.length() == 0)
			return Collections.emptySet();
		else {
			Set<Integer> conceptIds = new HashSet<Integer>();
			for (String cid : string.split(";"))
				conceptIds.add(Integer.parseInt(cid));
			return conceptIds;
		}
	}

	public boolean isUsedCheat() {
		return usedCheat;
	}

	public void setUsedCheat(boolean usedCheat) {
		this.usedCheat = usedCheat;
	}

	public String getSourceCode()
	{
		return sourceCode;
	}

	public void setSourceCode(String sourceCode)
	{
		this.sourceCode = sourceCode;
	}

	public String getSourceName()
	{
		return sourceName;
	}

	public void setSourceName(String sourceName)
	{
		this.sourceName = sourceName;
	}

	public int getSourceFrequency()
	{
		return sourceFrequency;
	}

	public void setSourceFrequency(int sourceFrequency)
	{
		this.sourceFrequency = sourceFrequency;
	}

	public Set<Integer> getSourceAutoAssignedConceptIds()
	{
		return sourceAutoAssignedConceptIds;
	}

	public void setSourceAutoAssignedConceptIds(Set<Integer> sourceAutoAssignedConceptIds)
	{
		this.sourceAutoAssignedConceptIds = sourceAutoAssignedConceptIds;
	}

	public List<Pair<String, String>> getSourceAdditionalInfo()
	{
		return sourceAdditionalInfo;
	}

	public void setSourceAdditionalInfo(List<Pair<String, String>> sourceAdditionalInfo)
	{
		this.sourceAdditionalInfo = sourceAdditionalInfo;
	}

	public String getFieldID()
	{
		return fieldID;
	}

	public void setFieldID(String fieldID)
	{
		this.fieldID = fieldID;
	}

	public String getFieldDescription()
	{
		return fieldDescription;
	}

	public void setFieldDescription(String fieldDescription)
	{
		this.fieldDescription = fieldDescription;
	}

	public boolean isFieldMapping()
	{
		return fieldMapping;
	}

	public void setFieldMapping(boolean fieldMapping)
	{
		this.fieldMapping = fieldMapping;
	}

	public static String getAdditionalInfoPrefix()
	{
		return ADDITIONAL_INFO_PREFIX;
	}

	public static void setAdditionalInfoPrefix(String additionalInfoPrefix)
	{
		ADDITIONAL_INFO_PREFIX = additionalInfoPrefix;
	}
}
