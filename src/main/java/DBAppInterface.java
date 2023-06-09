import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public interface DBAppInterface {

    void init() throws ClassNotFoundException, IOException;

    void createTable(String tableName, String clusteringKey, Hashtable<String,String> colNameType, Hashtable<String,String> colNameMin, Hashtable<String,String> colNameMax) throws DBAppException, IOException;

    void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException, ParseException;

    void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException,  IOException, ParseException;

    void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ParseException;

    void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException;

    Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException;


}
