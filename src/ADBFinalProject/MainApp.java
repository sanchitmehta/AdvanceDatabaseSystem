package ADBFinalProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author Sanchit Mehta and Pranav Chaphekar
 */
public class MainApp {

  public static void main(String args[]) throws FileNotFoundException {
    InputParser inputParser = new InputParser();
    if (args.length == 1) {
      File file = new File(args[0]);
      Scanner sc = new Scanner(file);
      while (sc.hasNextLine()) {
        inputParser.parse(sc.nextLine());
      }
    } else {
      System.out.println(
          "Wrong Input provided. Please run the program as: javac MainApp.java input_file_name");
      System.exit(0);
    }
  }
}