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

public class DomoticEedomus
{
	final static String 			actCaract		= "periph.caract";
	final static String 			actHisto		= "periph.history";
	final static String 			stdFormatDate	= "yyyy-MM-dd HH:mm:ss";
	static private Connection		cDB				= null;
	static private JSONObject		jsonConfig		= null;
	static private LinkedHashSet	setScript		= null;
	static private String 			urlLocal		= "";
	static private String 			urlCloud		= "";
	static private String 			apiUser			= "";
	static private String 			apiSecret		= "";
	static private String 			idInternalTemp	= "";
	static private String 			idExternalTemp	= "";
	static private String 			idConsoPower	= "";
	static private String 			configFile		= "config.txt";
	static private String 			localFile		= "eedomus.sql";
	static private String 			fileNewLine		= "\n";
	
	private	static String		formatQuery(boolean cloud, String action, String idPeripherique)
	{
		String query	= "";
		String url		= "";
		
		if (cloud)
			{
			url = urlCloud;
			}
		else
			{
			url = urlLocal;
			}
		query = url + "?api_user="+ apiUser +"&api_secret=" + apiSecret + "&action=" + action + "&periph_id=" + idPeripherique;
		return query;
	}
	
	public	static String		GetCurrentValue(String idPeriph) throws IOException, JSONException, SQLException, ParseException
	{
		String			query		= "";
		String			returnValue	= "";
		
		query	=	formatQuery(false, actCaract, idPeriph);

	    JSONObject json = readJsonFromUrl(query);
	    if (json != null)
	    {
	    	if (json.getInt("success") == 1)
	    	{
			    JSONObject jsonBody = json.getJSONObject("body");

			    returnValue		= jsonBody.getString("last_value");
	    		
	    	}
	    }
	    
		return returnValue;
	}

	public	static String[][]	GetHistoricalValues(String idPeriph) throws IOException, JSONException, SQLException, ParseException
	{
		String			query		= "";
		String[][]		returnValue	= null;
		String			values		= "";
		String[]		value_list = null;
		
		query	=	formatQuery(true, actHisto, idPeriph);

	    JSONObject json = readJsonFromUrl(query);
	    if (json != null)
	    {
	    	if (json.getInt("success") == 1)
	    	{
			    JSONObject jsonBody = json.getJSONObject("body");
		    	values = jsonBody.getJSONArray("history").toString();
		    	values = values.replace("[", "");
		    	values = values.replace("]", "");
		    	values = values.replace("\"", "");
		    	value_list = values.split(",");
		    	returnValue	= new String[value_list.length/2][2];
		    	if ((value_list.length > 0) &&
		    			((value_list.length % 2) == 0))
		    	{
			    	for(int i = 0 ; i < value_list.length ; i++)
		    		{
		    		// Value itself
		    		returnValue[i/2][0] = value_list[i].toString();
		    		i++;
		    		// Date when it has been pulled out
		    		returnValue[i/2][1] = value_list[i].toString();
		    		}
		    	}
	    	}
	    }

		return returnValue;
	}

	public	static void			logHistoDB(String idPeripherique) throws JSONException, IOException, SQLException, ParseException
	{
		String[][]	values	= null;
		String	temperature	= "";
		String	dateCollecte	= "";
		Date	now;	
		
	    values = null;
	    values = GetHistoricalValues(idPeripherique);
	    if (values != null)
	    {
		    now	= new Date();
	    	for (int i = 0 ; i < values.length ; i++)
			{
				temperature = values[i][0];
				dateCollecte = values[i][1];
			    System.out.println("pulled at " + now + " for periph " + idPeripherique + " : " + temperature + " ; " + dateCollecte);
			    String sqlInsert = "insert into ttemperature values ('" +
			    		idPeripherique + "', '" + dateCollecte + "', " + temperature + ") ON CONFLICT DO NOTHING;";
			    writeRepository(sqlInsert);
			}
	    }
		
	}
	
	private	static String		readAll(Reader rd) throws IOException
	  {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }

	private	static JSONObject	readJsonFromUrl(String url) throws IOException, JSONException, SQLException, ParseException
	  {
		Date			now;
		Date			lastCall;
		long			timeElapsed;
		boolean			createperiph	= false;
		String 			sqlUpdate 		= "";
		String 			idPeriph 		= "";

	    now	= new Date();
    	idPeriph = url.substring(url.indexOf("&periph_id=")+11);
	    if (url.indexOf(urlCloud) != -1)
	    {
	    	lastCall = getLastCallFromRepository(idPeriph);
	    	if (lastCall == null)
	    	{
	        	createperiph = true;
	    		
	    	}
	    	else
	    	{
	            timeElapsed = (now.getTime() - lastCall.getTime())/1000;
	            if (timeElapsed < 2*60)
	            {
		            System.out.println("Last call to cloud webservice " + timeElapsed + " second(s) ago.");
		  	      	JSONObject json = null;
		  	      	return json;
	            }
	    		
	    	}
	    }
		InputStream is = new URL(url).openStream();
	    /* Log last call if cloud url pinged */
	    if (url.indexOf(urlCloud) != -1)
	    {
	    	SimpleDateFormat format	= new SimpleDateFormat(stdFormatDate);
	    	if (cDB != null)
	    	{
		    	if (createperiph)
		    	{
			    	sqlUpdate = "insert into tlastcalleedomus (idperiph, lastcall) values ('" + idPeriph + "', '" + format.format(now) + "');";
		    	}
		    	else
		    	{
			    	sqlUpdate = "update tlastcalleedomus set lastcall = '" + format.format(now) + "' where idperiph = '" + idPeriph + "';";
		    	}
		    	writeRepository(sqlUpdate);
	    	}
	    }
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	      String jsonText = readAll(rd);
	      JSONObject json = new JSONObject(jsonText);
	      return json;
	    } finally {
	      is.close();
	    }
	  }

	private	static boolean		ConnectRepository(String DBname, String DBuser, String DBpassword) throws IOException, SQLException
	{
		boolean		result	=	false;
		
		try
		{
			//Class.forName("org.postgresql.Driver");
		    cDB = DriverManager.getConnection(DBname, DBuser, DBpassword);
		    cDB.setAutoCommit(true);
		    result	=	true;
		}
		catch (SQLException eDB)
		{
			System.err.println("Connexion to DB impossible, switching to log file.");
			cDB = null;
			try
			{
				// Do the script and config file exist? If not create them.
				File	fConfig	=	new File(configFile);
				if (!fConfig.exists())
				{
					fConfig.createNewFile();
				}
				else
				{
			        FileReader	fr = new FileReader(fConfig);
					if ((fr != null) && (jsonConfig == null))
					{
						BufferedReader	br		=	new BufferedReader(fr); 
						String 			line	=	"";
						String 			config	=	"";
						
						while((line = br.readLine()) != null)
						{ 
							config	=	config + line; 
						}
						jsonConfig = new JSONObject(config);
						br.close();
						fr.close();
					}
				    result	=	true;
				}
				File	fExist	=	new File(localFile);
				if (!fExist.exists())
				{
					fExist.createNewFile();
				}
		        FileReader	fr = new FileReader(fExist);
		        if (fr != null)
		        {
		        	setScript	=	new LinkedHashSet();
					BufferedReader			br			=	new BufferedReader(fr); 
					String 					line		=	"";
					
					while((line = br.readLine()) != null)
					{ 
						setScript.add(line); 
					}
				    fr.close();
		        }
			}
			catch (Exception eFile)
			{
				eFile.printStackTrace();
				System.err.println(eFile.getClass().getName()+": "+eFile.getMessage());
				System.exit(0);
			}
		}
		catch (Exception eGenerale)
		{
			eGenerale.printStackTrace();
			System.err.println(eGenerale.getClass().getName()+": "+eGenerale.getMessage());
			System.exit(0);
		}
		  
		return result;
	}
	
	private	static boolean		DisconnectRepository() throws IOException, SQLException, JSONException
	{
	    boolean	result	= true;
	      
	    if (cDB != null)
	    {
		    cDB.close();
	    }
	    /* Write a lastcall if under file log */
	    else
	    {
		    if (jsonConfig != null)
		    {
				File	fConfig	=	new File(configFile);
			    Date	now		=	new Date();
		        FileWriter	fw = new FileWriter(fConfig, false);
		    	SimpleDateFormat format	= new SimpleDateFormat(stdFormatDate);
		        jsonConfig.put("lastcall", format.format(now)); 
		    	fw.write(jsonConfig + fileNewLine);
			    fw.close();
		    }
		    /* Save the SQL script */
			File	fScript	=	new File(localFile);
	        FileWriter fw = new FileWriter(fScript);
	        if (fw != null)
	        {
	        	Iterator	lineScript	=	setScript.iterator();
	        	while (lineScript.hasNext())
	        	{
			    	fw.write(lineScript.next() + fileNewLine);
	        	}
				fw.close();
	        }
	    }
	    
	    return result;
	}

	private	static void			writeRepository(String	sqlQuery) throws IOException, SQLException
	{
	      if (cDB != null)
	      {
	    	  Statement		stmt	=	null;
	    	  
			  stmt	=	cDB.createStatement();
			  stmt.executeUpdate(sqlQuery);
		      stmt.close();
	      }
	      else
	      {
				setScript.add(sqlQuery); 
	      }

	}

	private	static String		getConfigFromRepository(String key) throws IOException, SQLException, JSONException
	{
		String		value	= "";
		
		if (cDB != null)
		{
			Statement	stmt 	= null;
			ResultSet	rs		= null; 
		    stmt = cDB.createStatement();
		    String sqlSelect = "select value from tconfig where key = '" + key + "';";
		    try
		    {
		    	rs = stmt.executeQuery(sqlSelect);
		        if (rs.next())
		        {
		        	value = rs.getString("value");
		        }
		    }
		    finally
		    {
		        if (rs != null)		{ rs.close();}
		        if (stmt != null)	{ stmt.close();}
		    }
			
		}
		else
		{
			value		= jsonConfig.getString(key);
		}
		
		return value;
	}
	
	private	static Date			getLastCallFromRepository(String idPeriph) throws SQLException, JSONException, ParseException
	{
		Date				lastCall		= null;
		SimpleDateFormat	sdf;
		
		if (cDB != null)
		{
			Statement	stmt 	= null;
			ResultSet	rs		= null; 
		    stmt = cDB.createStatement();
		    String sqlSelect = "select lastcall from tlastcalleedomus where idperiph = '" + idPeriph + "';";
		    try
		    {
		    	rs = stmt.executeQuery(sqlSelect);
		        if (rs.next())
		        {
		        	lastCall = rs.getTimestamp("lastcall");
		        }
		    }
		    finally
		    {
		        if (rs != null)		{ rs.close();}
		        if (stmt != null)	{ stmt.close();}
		    }
			
		}
		else if (jsonConfig != null)
		{
			sdf				= new SimpleDateFormat(stdFormatDate);
			lastCall		= sdf.parse(jsonConfig.getString("lastcall"));
		}
		
		return lastCall;
	}
	
	public static void			main(String[] args) throws IOException, JSONException, SQLException, InterruptedException, ParseException
	{
		String 			DBname			= "";
		String 			DBuser			= "";
		String 			DBpassword		= "";
		/* Requires 3 mandatory params :
		 * 		- name of the DB (only postgreSQL at this stage)
		 * 		- user name
		 * 		- password
		 * In case there are not provided, a script will be written as output  
		 */
		DBname			=	args[0];
		DBuser			=	args[1];
		DBpassword		=	args[2];
		if ((DBname == "") && (DBuser == "") && (DBpassword == ""))
		{
			DBname			= "*";
			DBuser			= "*";
			DBpassword		= "*";
			
		}
		/* Load config */
		if (ConnectRepository(DBname, DBuser, DBpassword))
		{
			urlLocal		=	getConfigFromRepository("urlLocal");
			urlCloud		=	getConfigFromRepository("urlCloud");
			apiUser			=	getConfigFromRepository("apiUser");
			apiSecret		=	getConfigFromRepository("apiSecret");
			idInternalTemp	=	getConfigFromRepository("idInternalTemp");
			idExternalTemp	=	getConfigFromRepository("idExternalTemp");
			idConsoPower	=	getConfigFromRepository("idConsoPower");
			
		    /* Get internal temp */
		    logHistoDB(idInternalTemp);
		    /* Get external temp */
		    //Tempo 500ms, pour temporiser le service cloud
		    Thread.sleep(500);
		    logHistoDB(idExternalTemp);
		    /* Get power consumption */
		    //Tempo 500ms, pour temporiser le service cloud
		    Thread.sleep(500);
		    logHistoDB(idConsoPower);

		    DisconnectRepository();
		}
		else
		{
			System.out.println("Connexion failed");
		}

	}

}


