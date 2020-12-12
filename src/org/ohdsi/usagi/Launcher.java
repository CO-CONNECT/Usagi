package org.ohdsi.usagi;

import java.io.File;

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
		ImportData.ImportSettings t = new ImportData.ImportSettings();


		if (args.length>0 && args[0].equals("build"))
		{

			System.out.println("gg");

			Global.commandLineinitiate(t, true);

			String vocabFolder = t.vocabFolder;
			VocabVersionGrabber vocabVersionGrabber = new VocabVersionGrabber();
			vocabVersionGrabber.grabVersion(vocabFolder);

			BerkeleyDbBuilder berkeleyDbBuilder = new BerkeleyDbBuilder();
			berkeleyDbBuilder.buildIndex(vocabFolder, null, null);

			LuceneIndexBuilder luceneIndexBuilder = new LuceneIndexBuilder();
			luceneIndexBuilder.buildIndex(vocabFolder, null, null);
		}
		if (args.length>0 && args[0].equals("run"))
		{
			Global.commandLineinitiate(t, false);

			importData.process(t);
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
}
