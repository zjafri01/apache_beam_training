package com.example.training;

import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;

class LinesFilter extends DoFn<String ,String> {

    @ProcessElement
    public void processElement(ProcessContext c){
        String line =c.element();
        String[] arr =line.split(",");

        StringBuilder serial_masked = new StringBuilder(arr[0]);
        for(int i=0; i<arr[0].length(); i++){
            if(i>1){
                serial_masked.setCharAt(i,'X');
            }
        }

        arr[0]= String.valueOf(serial_masked);


        StringBuilder name_masked = new StringBuilder(arr[2]);
        for(int j=0; j<arr[2].length(); j++){
            if(j>1){
                name_masked.setCharAt(j,'X');
            }
        }

        arr[2]= String.valueOf(name_masked);

        line = arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+","+arr[0].hashCode();

        c.output(line);
    }
}
