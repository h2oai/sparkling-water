package org.apache.spark.ml.spark.models.gm;

import water.util.TwoDimTable;

public class ClusteringUtils {

    static public TwoDimTable create2DTable(
            GaussianMixtureModel.GaussianMixtureOutput output,
            double[][] data,
            String colHeaderForRowHeader,
            String[] colHeaders,
            String name) {
        if(data == null || data.length == 0) {
            return new TwoDimTable(name, null, new String[] {}, new String[]{}, new String[]{},
                    new String[]{}, colHeaderForRowHeader);
        }

        String[] rowHeaders = new String[data.length];
        for(int i = 0; i < rowHeaders.length; i++){
            rowHeaders[i] = String.valueOf(i+1);
        }

        String[] colTypes = new String[colHeaders.length];
        String[] colFormats = new String[colHeaders.length];
        for (int i = 0; i < data[0].length; ++i) {
            colTypes[i] = "double";
            colFormats[i] = "%f";
        }

        TwoDimTable table = new TwoDimTable(name, null, rowHeaders, colHeaders, colTypes, colFormats, colHeaderForRowHeader);

        for (int i=0; i<data.length; ++i) {
            for (int j=0; j<data[i].length; ++j) {
                table.set(i, j, data[i][j]);
            }
        }
        return table;
    }

}
