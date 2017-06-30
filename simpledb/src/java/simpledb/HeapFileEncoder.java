package simpledb;

import java.io.*;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * HeapFileEncoder reads a comma delimited text file or accepts an array of
 * tuples and converts it to pages of binary data in the appropriate format for
 * simpledb heap pages Pages are padded out to a specified length, and written
 * consecutive in a data file.
 */

public class HeapFileEncoder {
	public static final String NULL_STRING = "";

	/**
	 * Convert the specified tuple list (with only integer fields) into a binary
	 * page file. <br>
	 *
	 * The format of the output file will be as specified in HeapPage and
	 * HeapFile.
	 *
	 * @see HeapPage
	 * @see HeapFile
	 * @param tuples
	 *            the tuples - a list of tuples, each represented by a list of
	 *            integers that are the field values for that tuple.
	 * @param out
	 *            The output file to write data to
	 * @param npagebytes
	 *            The number of bytes per page in the output file
	 * @param numFields
	 *            the number of fields in each input tuple
	 * @param withHeader
	 * 				whether first record constitutes a header
	 * @throws IOException
	 *             if the temporary/output file can't be opened
	 */
	public static void convert(ArrayList<ArrayList<Integer>> tuples, BufferedOutputStream out, int npagebytes, int numFields, boolean withHeader)
			throws IOException {
		// write out tuples to simple csv format
		File outFile = File.createTempFile("tuples", ".csv");

		FileWriter sw = new FileWriter(outFile);
		BufferedWriter bw = new BufferedWriter(sw);
		for (ArrayList<Integer> tuple : tuples) {
			int writtenFields = 0;
			for (Integer field : tuple) {
				writtenFields++;
				if (writtenFields > numFields) {
					bw.close();
					throw new RuntimeException(
							"Tuple has more than " + numFields + " fields: (" + Utility.listToString(tuple) + ")");
				}
				bw.write(String.valueOf(field));
				if (writtenFields < numFields) {
					bw.write(',');
				}
			}
			bw.write('\n');
		}
		bw.close();

		// read in csv file as dat binary data and write to out
		convert(outFile, out, npagebytes, ',', new HashSet<String>(), withHeader);
	}

	private static Map<String, Type> getTypes(List<String> columns, Iterable<CSVRecord> records, Set<String> nullStrings) {
		// linked hash map has predictable iteration order
		// in particular, it retrieves keys in the order they are put in
		// we want this to make sure columns aren't in a different order than the file
		// they were read from (can be confusing, and messes up tests)
		Map<String, Type> types = new LinkedHashMap<>();

		for (CSVRecord record : records) {
			Map<String, String> recordMap = record.toMap();
			for (String key : columns) {
				String val = recordMap.get(key);
				if (val == null ||
					(types.containsKey(key) && types.get(key) == null) ||
					nullStrings.contains(val)) {
					continue;
				}

				Type type;
				try {
					Integer.parseInt(val);
					type = Type.INT_TYPE;
				} catch (NumberFormatException e) {
					try {
						Double.parseDouble(val);
						type = Type.DOUBLE_TYPE;
					} catch (NumberFormatException ee) {
						type = null;
					}
				}

				if (!types.containsKey(key)) {
					types.put(key, type);
				} else {
					if (!types.get(key).equals(type)) {
						types.put(key, null);
					}
				}
			}
		}
		return types;
	}

	private static String[] getUniqueFields(File inFile) throws IOException {
		CSVParser parser = new CSVParser(new BufferedReader(new FileReader(inFile)),
				CSVFormat.EXCEL.withNullString(NULL_STRING));
		// first record used as header
		CSVRecord header = parser.iterator().next();
		List<String> uniqueFields = new ArrayList<String>();
		for(int i = 0; i < header.size(); i++) {
			String col = header.get(i);
			if (!uniqueFields.contains(col)) {
				// we can add it directly
				uniqueFields.add(col);
			} else {
				// disambiguate by appending index
				uniqueFields.add(col + "_" + i);
			}
		}
		return uniqueFields.toArray(new String[0]);
	}

	/**
	 * Convert the specified input text file into a binary page file. <br>
	 * Assume format of the input file is (note that only integer fields are
	 * supported):<br>
	 * int,...,int\n<br>
	 * int,...,int\n<br>
	 * ...<br>
	 * where each row represents a tuple.<br>
	 * <p>
	 * The format of the output file will be as specified in HeapPage and
	 * HeapFile.
	 *
	 * @see HeapPage
	 * @see HeapFile
	 * @param inFile
	 *            The input file to read data from
	 * @param out
	 *            The output file to write data to
	 * @param npagebytes
	 *            The number of bytes per page in the output file
	 * @throws IOException
	 *             if the input/output file can't be opened or a malformed input
	 *             line is encountered
	 */
	public static TupleDesc convert(File inFile, BufferedOutputStream out, int npagebytes,
									char fieldSeparator, Set<String> nullStrings, boolean withHeaders) throws IOException {
		CSVParser typeParser;
		try {
			typeParser = new CSVParser(new BufferedReader(new FileReader(inFile)),
					CSVFormat.EXCEL.withNullString(NULL_STRING).withHeader());
		} catch (IllegalArgumentException ex) {
			// if header values are not unique, we can get an exception. We'll try to make progress
			// by creating unique header names based on first tuple
			String[] uniqueFields = getUniqueFields(inFile);
			typeParser = new CSVParser(new BufferedReader(new FileReader(inFile)),
					CSVFormat.EXCEL.withNullString(NULL_STRING).withHeader(uniqueFields));
		}
		// provide headers in appropriate order, based on underlying csv....
		List<String> columns = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : typeParser.getHeaderMap().entrySet()) {
			columns.add(entry.getValue(), entry.getKey());
		}
		// map is returned in appropriate order based on columns
		Map<String, Type> types = getTypes(columns, typeParser, nullStrings);

		// Get the list of integer fields.
		boolean ignoredFields = false;
		ArrayList<String> fields = new ArrayList<>();
		for (String key : types.keySet()) {
			Type type = types.get(key);
			if (Type.INT_TYPE.equals(type)) {
				fields.add(key);
			} else {
				String typeStr;
				if (type == null) {
					typeStr = "mixed types";
				} else {
					typeStr = type.toString();
				}
				System.err.format("Warning: Ignoring field %s (%s).\n", key, typeStr);
				ignoredFields = true;
			}
		}
		if (ignoredFields) {
			System.err.format("Warning: Some fields ignored. Only integer fields are supported.\n");
		}
		int numFields = fields.size();
		Type[] typeAr = new Type[numFields];
		for (int i = 0; i < numFields; i++) {
			typeAr[i] = types.get(fields.get(i));
		}

		TupleDesc ret = new TupleDesc(typeAr, fields.toArray(new String[0]));

		int nrecbytes = 0;
		for (int i = 0; i < numFields; i++) {
			nrecbytes += typeAr[i].length;
		}
		// floor comes for free
		final int nrecords = (npagebytes * 8) / (nrecbytes * 8 + 1); 

		// per record, we need one bit; there are nrecords per page, so we need
		// nrecords bits, i.e., ((nrecords/32)+1) integers.
		int nheaderbytes = (nrecords / 8);
		if (nheaderbytes * 8 < nrecords)
			nheaderbytes++; // ceiling
		final int nheaderbits = nheaderbytes * 8;

		int recordcount = 0;
		int npages = 0;
		
		System.err.println("Writing heap file from CSV input.");
		System.err.format("%d byte header\n", nheaderbytes);
		System.err.format("%d byte records, %d records per page, %d bytes total\n", 
				nrecbytes, nrecords, nrecbytes * nrecords);
		
		final ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
		final ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);

		final DataOutputStream headerStream = new DataOutputStream(headerBAOS);
		final DataOutputStream pageStream = new DataOutputStream(pageBAOS);

		CSVFormat formatter = CSVFormat.EXCEL.withNullString(NULL_STRING);
		if (withHeaders) {
			formatter = formatter.withFirstRecordAsHeader();
		} else {
			// use the fields we extracted before as headers
			formatter = formatter.withHeader(fields.toArray(new String[0]));
		}
		final CSVParser parser = new CSVParser(new BufferedReader(new FileReader(inFile)), formatter);
		final Iterator<CSVRecord> records = parser.iterator();

		boolean done = false;
		while (!done) {
			if (records.hasNext()) {
				CSVRecord r = records.next();

				// Write out record fields.
				for (String fieldName : fields) {
					String field = r.get(fieldName);
					Type fieldType = types.get(fieldName);

					switch(fieldType) {
					case INT_TYPE:
						try {
							if (field == null || nullStrings.contains(field)) {
								field = Integer.toString(Type.MISSING_INTEGER);
							}
							pageStream.writeInt(Integer.parseInt(field));
						} catch (NumberFormatException e) {
							System.err.format("Bad record (line=%d): %s", r.getRecordNumber(), r);
							throw new IOException("Conversion failed.");
						}
						break;
					case STRING_TYPE:
						if (field == null || nullStrings.contains(field)) {
							field = Type.MISSING_STRING;
						}
						int overflow = Type.STRING_LEN - field.length();
						if (overflow < 0) {
							System.err.format("String too long (line=%d): %s", r.getRecordNumber(), r);
							throw new IOException("Conversion failed.");
						}
						pageStream.writeInt(field.length());
						pageStream.writeBytes(field);
						while (overflow-- > 0) {
							pageStream.write((byte) 0);
						}
						break;
					case DOUBLE_TYPE:
						try {
							double v = field == null ? Type.MISSING_DOUBLE : Double.parseDouble(field);
							pageStream.writeDouble(v);
						} catch (NumberFormatException e) {
							System.err.format("Bad record (line=%d): %s", r.getRecordNumber(), r);
							throw new IOException("Conversion failed.");
						}
						break;
					default:
						throw new RuntimeException("Unexpected type.");
					}
				}
				recordcount++;
			} else {
				done = true;
			}

			// if we wrote a full page of records, or if we're done altogether,
			// write out the header of the page.
			//
			// in the header, write a 1 for bits that correspond to records
			// we've
			// written and 0 for empty slots.
			//
			// when we're done, also flush the page to disk, but only if it has
			// records on it. however, if this file is empty, do flush an empty
			// page to disk.
			if (recordcount >= nrecords || done && recordcount > 0 || done && npages == 0) {
				int i = 0;
				byte headerbyte = 0;

				for (i = 0; i < nheaderbits; i++) {
					if (i < recordcount)
						headerbyte |= (1 << (i % 8));

					if (((i + 1) % 8) == 0) {
						headerStream.writeByte(headerbyte);
						headerbyte = 0;
					}
				}

				if (i % 8 > 0)
					headerStream.writeByte(headerbyte);

				// pad the rest of the page with zeroes

				for (i = 0; i < (npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
					pageStream.writeByte(0);

				// write header and body to file
				headerStream.flush();
				pageStream.flush();
				headerBAOS.writeTo(out);
				pageBAOS.writeTo(out);

				// reset header and body for next page
				headerBAOS.reset();
				pageBAOS.reset();

				recordcount = 0;
				npages++;
			}
		}

		System.err.format("%d pages\n", npages);
		return ret;
	}
}
