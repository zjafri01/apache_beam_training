package com.example.training;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
class Data {

    String username;
    String identifier;
    String first_name;
    String last_name;

    public static Data fromTableRow(TableRow row){
        Data data = new Data();
        data.username=(String) row.get("Username");
        data.identifier=(String) row.get("Identifier");
        data.first_name=(String) row.get("First_Name");
        data.last_name=(String) row.get("Last_Name");
        return data;

    }



}