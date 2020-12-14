package org.ohdsi.usagi;

import java.io.*;
import java.util.Collections;
import java.util.Properties;
import java.util.Vector;

import org.ohdsi.usagi.dataImport.ImportData;
import org.ohdsi.usagi.indexBuilding.BerkeleyDbBuilder;
import org.ohdsi.usagi.indexBuilding.IndexBuildCoordinator;
import org.ohdsi.usagi.indexBuilding.LuceneIndexBuilder;
import org.ohdsi.usagi.indexBuilding.VocabVersionGrabber;
import org.ohdsi.usagi.ui.Global;
import org.ohdsi.usagi.ui.UsagiMain;

public class Launcher {
	private final static int	MIN_HEAP	= 1000;

	public static void main(String[] args) throws Exception {

		ImportData importData = new ImportData();
		ImportData.ImportSettings importSettings = new ImportData.ImportSettings();


		if (args.length>0 && args[0].equals("build"))
		{

            setProperties(args[1], importSettings);

            Global.commandLineinitiate(importSettings, true);

			String vocabFolder = importSettings.vocabFolder;
			VocabVersionGrabber vocabVersionGrabber = new VocabVersionGrabber();
			vocabVersionGrabber.grabVersion(vocabFolder);

			BerkeleyDbBuilder berkeleyDbBuilder = new BerkeleyDbBuilder();
			berkeleyDbBuilder.buildIndex(vocabFolder, null, null);

			LuceneIndexBuilder luceneIndexBuilder = new LuceneIndexBuilder();
			luceneIndexBuilder.buildIndex(vocabFolder, null, null);
		}
		else if (args.length>0 && args[0].equals("run"))
		{
			setProperties(args[1], importSettings);
			Global.commandLineinitiate(importSettings, false);

			importData.process(importSettings);
		}

		else
		{

			float heapSizeMegs = (Runtime.getRuntime().maxMemory() / 1024) / 1024;

			if (heapSizeMegs > MIN_HEAP)
			{
				System.out.println("Launching with current VM");
				UsagiMain.main(args);
			} else
			{
				System.out.println("Starting new VM");
				String pathToJar = UsagiMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
				ProcessBuilder pb = new ProcessBuilder("java", "-Xmx" + MIN_HEAP + "m", "-classpath", pathToJar, "org.ohdsi.usagi.ui.UsagiMain");
				pb.inheritIO();
				pb.redirectError(new File("ErrorStream.txt"));
				pb.start();
			}
		}


	}
	private static void  setProperties(String filename, ImportData.ImportSettings settings) throws IOException
	{
		InputStream input = new FileInputStream(filename);

		Properties prop = new Properties();

		// load a properties file
		prop.load(input);

		settings.usagiFolder = prop.getProperty("usagiFolder");
		settings.vocabFolder = prop.getProperty("vocabFolder");
		settings.sourceFile = prop.getProperty("sourceFile");
		settings.mappingFile = prop.getProperty("mappingFile");
		settings.sourceCodeColumn = prop.getProperty("sourceCodeColumn");
		settings.sourceNameColumn = prop.getProperty("sourceNameColumn");

		if (prop.getProperty("threshold")!=null)
			settings.threshold = Double.parseDouble(prop.getProperty("threshold"));

		if (prop.getProperty("filterDomains")!=null)
		{
			settings.filterDomains = new Vector<>();
			Collections.addAll(settings.filterDomains, prop.getProperty("filterDomains").split(","));
		}
		if (prop.getProperty("filterConceptClasses")!=null)
		{
			settings.filterConceptClasses = new Vector<>();
			Collections.addAll(settings.filterConceptClasses, prop.getProperty("filterConceptClasses").split(","));
		}
		if (prop.getProperty("filterVocabularies")!=null)
		{
			settings.filterVocabularies = new Vector<>();
			Collections.addAll(settings.filterVocabularies, prop.getProperty("filterVocabularies").split(","));
		}

	}
}
