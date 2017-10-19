package simpledb;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
	private class TableInfo {
		public TableInfo(DbFile file, String name, String pKeyField) {
			this.file = file;
			this.name = name;
			this.pKeyField = pKeyField;
		}
		public final DbFile file;
		public final String name;
		public final String pKeyField;
	}
	
	private final HashMap<Integer, TableInfo> tables;
	private final HashMap<String, Integer> idOfName;
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        tables = new HashMap<>();
        idOfName = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	int id = file.getId();
        tables.put(id, new TableInfo(file, name, pkeyField));
        idOfName.put(name, id);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }
    
    /**
     * Adds a new table to the catalog. Stored in a temp file.
     * @param tableName The name of the table.
     * @param pKeyField The primary key of the table.
     * @throws IOException 
     */
    public void addTable(String tableName, String pKeyField, TupleDesc schema) throws IOException {
    	File file = File.createTempFile(tableName, ".dat");
    	DbFile dbFile = new HeapFile(file, schema);
    	addTable(dbFile, tableName, pKeyField);
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        Integer id = idOfName.get(name);
        if (id == null) {
        	throw new NoSuchElementException();
        }
        return id;
    }
    
    private TableInfo getTable(int tableid) throws NoSuchElementException {
    	TableInfo t = tables.get(tableid);
        if (t == null) {
        	throw new NoSuchElementException();
        }
        return t;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        return getTable(tableid).file.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	return getTable(tableid).file;
    }

    public String getPrimaryKey(int tableid) {
    	return getTable(tableid).pKeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        return tables.keySet().iterator();
    }

    public String getTableName(int id) {
    	return getTable(id).name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        tables.clear();
    }

    /**
     * Writes a catalog to disk as a catalog file.
     * @param catalogFile
     */
    public void dumpSchema(File catalogFile) {
        try (PrintWriter wr = new PrintWriter(new FileWriter(catalogFile))) {
            for (TableInfo table : tables.values()) {
                ArrayList<String> tdStrs = new ArrayList<>();
                for (TDItem ti : table.file.getTupleDesc()) {
                    tdStrs.add(String.format("%s %s", ti.fieldName, ti.fieldType.toString()));
                }
                String tdStr = String.join(", ", tdStrs);
                wr.format("%s(%s)\n", table.name, tdStr);
            }
        } catch (IOException e) {
            System.err.format("Error: Failed to write catalog file: %s\n", catalogFile);
        }
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try (
        	BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
        ) {
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    try {
                    	types.add(Type.ofString(els2[1].trim()));
                    } catch (ParseException ex) {
                    	System.err.println(ex.getMessage());
                    	System.exit(-1);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        }
                        else {
                            System.err.println("Unknown annotation " + els2[2]);
                            System.exit(-1);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IndexOutOfBoundsException e) {
            System.err.println ("Invalid catalog entry : " + line);
            System.exit(-1);
        }
    }
}

