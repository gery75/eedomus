package domotic.eedomus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.logging.Logger;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.json.JSONException;
import org.json.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;


public class DomoticEedomus {
	static final String ACT_CARACT = "periph.caract";
	static final String ACT_HISTO = "periph.history";
	static final String FORMAT_DATE = "yyyy-MM-dd HH:mm:ss";
	static final String LAST_CALL = "lastcall";
	static final int QUERY_CLOUD_ELAPSED = 2 * 60; // In seconds
	public static final String DOMOTIC_ERR_MSG = "Domotic error.";
	private static final Logger LOGGER = Logger.getLogger(Thread.currentThread().getStackTrace()[0].getClassName());
	private static Connection cDB = null;
	private static JSONObject jsonConfig = null;
	private static LinkedHashSet<String> setScript = null;
	private static String urlLocal = "";
	private static String urlCloud = "";
	private static String apiUser = "";
	private static String apiSecret = "";
	private static String configFile = "config.txt";
	private static String localFile = "eedomus.sql";
	private static String fileNewLine = "\n";

	private DomoticEedomus() {
		/* Constructor that can be overriden */
	}

	private static String formatQuery(boolean cloud, String action, String idPeripherique) {
		String query;
		String url;

		if (cloud) {
			url = urlCloud;
		} else {
			url = urlLocal;
		}
		query = url + "?api_user=" + apiUser + "&api_secret=" + apiSecret + "&action=" + action + "&periph_id="
				+ idPeripherique;
		return query;
	}

	private static File openFile(String fileName) throws IOException {
		boolean fileExist;
		File fReturn = new File(fileName);
		if (!fReturn.exists()) {
			fileExist = fReturn.createNewFile();
			if (!fileExist) {
				fReturn = null;
			}
		}
		return fReturn;
	}

	public static String getCurrentValue(String idPeriph)
			throws DomoticException
	{
		String query;
		String returnValue = "";

		query = formatQuery(false, ACT_CARACT, idPeriph);

		try
		{
			JSONObject json = readJsonFromUrl(query);
			if ((json != null) && (json.getInt("success") == 1)) {
				JSONObject jsonBody = json.getJSONObject("body");
	
				returnValue = jsonBody.getString("last_value");
			}
		}
		catch (IOException | JSONException | SQLException | ParseException excp)
		{
			LOGGER.warning(excp.getMessage());
			throw new DomoticException(DOMOTIC_ERR_MSG, excp);
		}

		return returnValue;
	}

	public static String[][] getHistoricalValues(String idPeriph)
			throws DomoticException {
		String query;
		String[][] returnValue = null;
		String values;

		try
		{
			ObjectMapper mapper = new ObjectMapper();
			query = formatQuery(true, ACT_HISTO, idPeriph);

			JSONObject json = readJsonFromUrl(query);
			if ((json != null) && (json.getInt("success") == 1)) {
				JSONObject jsonBody = json.getJSONObject("body");
				values = jsonBody.getJSONArray("history").toString();
				returnValue = mapper.readValue(values, String[][].class);
			}

		}
		catch (IOException | JSONException | SQLException | ParseException excp)
		{
			LOGGER.warning(excp.getMessage());
			throw new DomoticException(DOMOTIC_ERR_MSG, excp);
		}

		return returnValue;
	}

	public static void logHistoDB(String idPeripherique)
			throws DomoticException {
		String[][] values;
		String temperature;
		String dateCollecte;
		Date now;

		try
		{
			values = getHistoricalValues(idPeripherique);
			if (values != null) {
				now = new Date();
				for (int i = 0; i < values.length; i++) {
					temperature = values[i][0];
					dateCollecte = values[i][1];
					LOGGER.info("pulled at " + now + " for periph " + idPeripherique + " : " + temperature + " ; "
							+ dateCollecte);
					String sqlInsert = "insert into ttemperature values ('" + idPeripherique + "', '" + dateCollecte + "', "
							+ temperature + ") ON CONFLICT DO NOTHING;";
					writeRepository(sqlInsert);
				}
			}
		}
		catch (IOException | SQLException excp)
		{
			LOGGER.warning(excp.getMessage());
			throw new DomoticException(DOMOTIC_ERR_MSG, excp);
		}

	}

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	private static JSONObject readJsonFromUrl(String url)
			throws IOException, JSONException, SQLException, ParseException {
		Date now;
		Date lastCall;
		long timeElapsed;
		boolean createperiph = false;
		String sqlUpdate = "";
		String idPeriph = "";

		now = new Date();
		idPeriph = url.substring(url.indexOf("&periph_id=") + 11);
		/*
		 * In case we pull the cloud web service, we may face some billing
		 * issues, we will wait for a timer
		 */
		if (url.indexOf(urlCloud) != -1) {
			lastCall = getLastCallFromRepository(idPeriph);
			if (lastCall == null) {
				createperiph = true;
			} else {
				timeElapsed = (now.getTime() - lastCall.getTime()) / 1000;
				if (timeElapsed < QUERY_CLOUD_ELAPSED) {
					LOGGER.warning("Last call to cloud webservice " + timeElapsed + " second(s) ago.");
					return null;
				}

			}
		}
		/* Log last call if cloud url pinged */
		if (url.indexOf(urlCloud) != -1) {
			SimpleDateFormat format = new SimpleDateFormat(FORMAT_DATE);
			if (cDB != null) {
				if (createperiph) {
					sqlUpdate = "insert into tlastcalleedomus (idperiph, lastcall) values ('" + idPeriph + "', '"
							+ format.format(now) + "');";
				} else {
					sqlUpdate = "update tlastcalleedomus set lastcall = '" + format.format(now) + "' where idperiph = '"
							+ idPeriph + "';";
				}
				writeRepository(sqlUpdate);
			}
		}
		/* Build the json object to be returned, reading the url */
		try (InputStream is = new URL(url).openStream()) {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			return new JSONObject(jsonText);
		}
	}

	private static boolean connectRepository(String dbName, String dbUser, String dbPassword)
			throws IOException, SQLException, JSONException {
		boolean result = false;

		try {
			cDB = DriverManager.getConnection(dbName, dbUser, dbPassword);
			cDB.setAutoCommit(true);
			result = true;
		} catch (SQLException eDB) {
			LOGGER.warning("Connexion to DB impossible, switching to log file.");
			cDB = null;
			/* If no DB connexion, then switch to file */
			// Do the script and config file exist? If not create them.
			File fConfig = openFile(configFile);
			if (fConfig == null) {
				return false;
			}
			/*
			 * Read the config file and populate the json object if not yet
			 * populated
			 */
			if (jsonConfig == null) {
				try (FileReader frConfig = new FileReader(fConfig); BufferedReader br = new BufferedReader(frConfig)) {
					String line;
					StringBuilder config = new StringBuilder("");

					while ((line = br.readLine()) != null) {
						config.append(line);
					}
					jsonConfig = new JSONObject(config.toString());
				}
			}
			/* Initiate the local script file */
			result = true;
			File fExist = openFile(localFile);
			if (fExist == null) {
				return false;
			}
			try (FileReader frScript = new FileReader(fExist); BufferedReader br = new BufferedReader(frScript)) {
				setScript = new LinkedHashSet<>();
				String line;
				while ((line = br.readLine()) != null) {
					setScript.add(line);
				}
			}
		}

		return result;
	}

	private static boolean disconnectRepository() throws IOException, SQLException, JSONException {
		boolean result = true;

		if (cDB != null) {
			cDB.close();
		}
		/* Write a lastcall if under file log */
		else {
			if (jsonConfig != null) {
				File fConfig = new File(configFile);
				Date now = new Date();
				FileWriter fw = new FileWriter(fConfig, false);
				SimpleDateFormat format = new SimpleDateFormat(FORMAT_DATE);
				jsonConfig.put(LAST_CALL, format.format(now));
				fw.write(jsonConfig + fileNewLine);
				fw.close();
			}
			/* Save the SQL script */
			File fScript = new File(localFile);
			FileWriter fw = new FileWriter(fScript);
			Iterator<String> lineScript = setScript.iterator();
			while (lineScript.hasNext()) {
				fw.write(lineScript.next() + fileNewLine);
			}
			fw.close();
		}

		return result;
	}

	private static void writeRepository(String sqlQuery) throws IOException, SQLException {
		if (cDB != null) {
			Statement stmt;

			stmt = cDB.createStatement();
			stmt.executeUpdate(sqlQuery);
			stmt.close();
		} else {
			setScript.add(sqlQuery);
		}

	}

	private static String getConfigFromRepository(String key) throws IOException, SQLException, JSONException {
		String value = "";

		if (cDB != null) {
			Statement stmt = null;
			ResultSet rs = null;
			stmt = cDB.createStatement();
			String sqlSelect = "select value from tconfig where key = '" + key + "';";
			try {
				rs = stmt.executeQuery(sqlSelect);
				if (rs.next()) {
					value = rs.getString("value");
				}
			} finally {
				if (rs != null) {
					rs.close();
				}
				stmt.close();
			}

		} else {
			value = jsonConfig.getString(key);
		}

		return value;
	}

	private static Date getLastCallFromRepository(String idPeriph) throws SQLException, JSONException, ParseException {
		Date lastCall = null;
		SimpleDateFormat sdf;

		if (cDB != null) {
			Statement stmt = null;
			ResultSet rs = null;
			stmt = cDB.createStatement();
			String sqlSelect = "select lastcall from tlastcalleedomus where idperiph = '" + idPeriph + "';";
			try {
				rs = stmt.executeQuery(sqlSelect);
				if (rs.next()) {
					lastCall = rs.getTimestamp(LAST_CALL);
				}
			} finally {
				if (rs != null) {
					rs.close();
				}
				stmt.close();
			}

		} else if (jsonConfig != null) {
			sdf = new SimpleDateFormat(FORMAT_DATE);
			lastCall = sdf.parse(jsonConfig.getString(LAST_CALL));
		}

		return lastCall;
	}

	public static void main(String[] args)
			throws DomoticException, IOException, JSONException, SQLException, InterruptedException, ParseException {
		String dbName;
		String dbUser;
		String dbPassword;
		String idInternalTemp;
		String idExternalTemp;
		String idConsoPower;

		/*
		 * Requires 3 mandatory params : - name of the DB (only postgreSQL at
		 * this stage) - user name - password In case there are not provided, a
		 * script will be written as output
		 */
		dbName = args[0];
		dbUser = args[1];
		dbPassword = args[2];
		if ((dbName == "") && (dbUser == "") && (dbPassword == "")) {
			dbName = "*";
			dbUser = "*";
		}
		/* Load config */
		if (connectRepository(dbName, dbUser, dbPassword)) {
			urlLocal = getConfigFromRepository("urlLocal");
			urlCloud = getConfigFromRepository("urlCloud");
			apiUser = getConfigFromRepository("apiUser");
			apiSecret = getConfigFromRepository("apiSecret");
			idInternalTemp = getConfigFromRepository("idInternalTemp");
			idExternalTemp = getConfigFromRepository("idExternalTemp");
			idConsoPower = getConfigFromRepository("idConsoPower");

			/* Get internal temp */
			logHistoDB(idInternalTemp);
			/* Get external temp */
			// Tempo 500ms, pour temporiser le service cloud
			Thread.sleep(500);
			logHistoDB(idExternalTemp);
			/* Get power consumption */
			// Tempo 500ms, pour temporiser le service cloud
			Thread.sleep(500);
			logHistoDB(idConsoPower);

			disconnectRepository();
		} else {
			LOGGER.info("Connexion failed");
		}

	}

}
