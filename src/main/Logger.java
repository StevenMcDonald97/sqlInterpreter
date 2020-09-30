package main;
/** Every interpreter has a Logger which directs debugging statements to an output file
 */


public class Logger {
//	create an object of databaseCatalog using schemas in schema.txt

		private static Logger instance = new Logger();

		private Logger(){

		};
	   
	   /** getInstance() returns the only instance of Logger which exists
	    * @return the only instance of Logger for the current program
	    */
	   public static Logger getInstance(){
	      return instance;
	   }

}