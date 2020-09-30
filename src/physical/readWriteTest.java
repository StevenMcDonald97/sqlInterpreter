package physical;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Arrays;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import main.*;

public class readWriteTest {
	private static final String queriesFile = "./queries.sql";

	public static void main(String[] args) {
		try {
			FileReader reader = new FileReader(queriesFile);
			CCJSqlParser parser = new CCJSqlParser(reader);
			Statement statement;
			ArrayList<String> schema;
			databaseCatalog cat = databaseCatalog.getInstance();
			
			while ((statement = parser.Statement()) != null) {
				Select select = (Select) statement;
				PlainSelect body = (PlainSelect) select.getSelectBody();
				Expression where = body.getWhere();
				String table = body.getFromItem().toString();
				schema = cat.getSchema(table);

				ScanOperator scan = new ScanOperator(table, schema);
//				System.out.println(scan.getNextTuple().getBody());
				scan.dump(0);
				
			}

			
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
