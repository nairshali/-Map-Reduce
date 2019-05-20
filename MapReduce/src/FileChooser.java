package MapReduce;

import java.io.File;
import java.util.Arrays;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileSystemView;

public class FileChooser {

	public static String filepath;

	public static String main() {

		JFileChooser jfc = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
		jfc.setDialogTitle("Multiple file and directory selection:");
		jfc.setMultiSelectionEnabled(true);
		jfc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);

		int returnValue = jfc.showOpenDialog(null);
		if (returnValue == JFileChooser.APPROVE_OPTION) {
			File[] files = jfc.getSelectedFiles();
			//System.out.println("Directories found\n");
			Arrays.asList(files).forEach(x -> {
				if (x.isDirectory()) {
					System.out.println(x.getName());
				}
			});
			//System.out.println("\n- - - - - - - - - - -\n");
			//System.out.println("Files Found\n");
			Arrays.asList(files).forEach(x -> {
				if (x.isFile()) {
					System.out.println(x.getPath());
					filepath = x.getPath();
				}
			});
		}
		return filepath;
	}
}
