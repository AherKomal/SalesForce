/**
 * Created by synerzip on 6/9/17.
 */

import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.*;
//import com.sforce.ws.*;

public class MainClass {

    public static void main(String[] args) {
        String userName = "komal.aher@gmail.com";
        String password = "Salesforce03h11EF5KRdOddR9MJrVH3c1LH";
        ConnectorConfig config = new ConnectorConfig();
        config.setUsername(userName);
        config.setPassword(password);
        PartnerConnection connection = null;

        try
        {
            List<String> lstIds = new ArrayList<String>();
            //Suppose you have filled lstIds with your ids.
            String query = "select Id, Name from lead ";

            String strIds = "";
            for(String ids:lstIds){
                if(strIds.equals("")){
                    strIds = "'" + ids + "'";
                }
                else{
                    strIds += ",'" + ids + "'";
                }
            }
            query += "where id in (" + strIds + ")";
            connection = Connector.newConnection(config);

            QueryResult result = connection.query(query);
            if(result.getSize()>0)
            {
                boolean done=false;
                int i=1;
                while(!done)
                {

                    for (SObject record : result.getRecords())
                    {
                        System.out.println("###### record.Id: " + (String)record.getField("Id"));
                        System.out.println("###### record.Name: " + (String)record.getField("Name"));
                    }
                    if (result.isDone()) {
                        done = true;
                    } else {
                        result = connection.queryMore(result.getQueryLocator());
                    }
                }
            }
        }
        catch(Exception ex)
        {
            System.out.println("Exception in main : " + ex);
        }
    }


}
