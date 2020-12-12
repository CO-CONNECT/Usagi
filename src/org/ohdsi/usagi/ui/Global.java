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
package org.ohdsi.usagi.ui;

import javax.swing.JFrame;

import org.ohdsi.usagi.BerkeleyDbEngine;
import org.ohdsi.usagi.UsagiSearchEngine;
import org.ohdsi.usagi.ui.actions.*;
import org.ohdsi.utilities.files.ReadTextFile;

import java.io.File;
import java.util.Vector;

public class Global {
	public static JFrame							frame;
	public static Mapping							mapping;
	public static UsagiSearchEngine					usagiSearchEngine;
	public static BerkeleyDbEngine					dbEngine;
	public static String							folder;
	public static String							filename;

	public static MappingDetailPanel				mappingDetailPanel;
	public static MappingTablePanel					mappingTablePanel;
	public static ConceptInformationDialog			conceptInformationDialog;
	public static UsagiStatusBar					statusBar;

	public static OpenAction						openAction;
	public static ApplyPreviousMappingAction		applyPreviousMappingAction;
	public static ImportAction						importAction;
	public static SaveAction						saveAction;
	public static SaveAsAction						saveAsAction;
	public static ApproveAction						approveAction;
	public static ApproveAllAction					approveAllAction;
	public static ClearAllAction					clearAllAction;
	public static ConceptInformationAction			conceptInfoAction;
	public static AthenaAction						athenaAction;
	public static GoogleSearchAction				googleSearchAction;
	public static AboutAction						aboutAction;
	public static ExportSourceToConceptMapAction	exportAction;
	public static ExportForReviewAction				exportForReviewAction;
	public static RebuildIndexAction				rebuildIndexAction;
	public static ExitAction						exitAction;
	public static String							vocabularyVersion;
	public static Vector<String> 					conceptClassIds;
	public static Vector<String> 					vocabularyIds;
	public static Vector<String>	 				domainIds;
	public static ShowStatsAction					showStatsAction;

	public static void commandLineinitiate()
	{

		Global.folder = folder;
		Global.usagiSearchEngine = new UsagiSearchEngine(Global.folder);
		Global.dbEngine = new BerkeleyDbEngine(Global.folder);
		if (Global.usagiSearchEngine.mainIndexExists()) {
			Global.usagiSearchEngine.openIndexForSearching(false);
			Global.dbEngine.openForReading();
		}
		Global.vocabularyVersion = loadVocabularyVersion(Global.folder);
		Global.conceptClassIds = loadVectorFromFile(Global.folder + "/ConceptClassIds.txt");
		Global.vocabularyIds = loadVectorFromFile(Global.folder + "/VocabularyIds.txt");
		Global.domainIds = loadVectorFromFile(Global.folder + "/DomainIds.txt");



	}
	public static String loadVocabularyVersion(String folder) {
		String versionFileName = folder + "/vocabularyVersion.txt";
		String version = "Unknown";
		if (new File(versionFileName).exists()) {
			for (String line : new ReadTextFile(versionFileName))
				version = line;
		}
		return version;
	}
	public static Vector<String> loadVectorFromFile(String fileName)
	{
		if (new File(fileName).exists())
		{
			Vector<String> vector = new Vector<>();
			for (String line : new ReadTextFile(fileName))
				vector.add(line);
			return vector;
		}
		else
			return new Vector<>();
	}
}
