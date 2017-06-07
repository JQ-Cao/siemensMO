package basic;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineSegment;
import org.geotools.referencing.GeodeticCalculator;
import org.geotools.referencing.datum.DefaultEllipsoid;

/**
 * Created by caojiaqing on 25/04/2017.
 */
public class MapUtil {
    private static final int DEFAULTSIZE = 100;

    /**
     *
     * @param lon
     * @param lat
     * @return 二级网格号Grid
     */
    private static int findGrid(double lon, double lat) {
        int xx = (int) (lon - 60);
        int yy = (int) (lat * 60 / 40);
        int x = (int) ((lon - (int) lon) / 0.125);
        int y = (int) (((lat * 60 / 40 - yy)) / 0.125);
        return yy * 10000 + xx * 100 + y * 10 + x;

    }


    /**
     * 按二级网格号获取二级网格的坐标范围
     *
     * @param gridNo
     * @return
     */
    private static Envelope getGridBound(int gridNo) {

        int yy = (int) (gridNo / 10000);
        int xx = (int) ((gridNo - yy * 10000) / 100);
        int y = (int) ((gridNo - yy * 10000 - xx * 100) / 10);
        int x = (int) ((gridNo - yy * 10000 - xx * 100 - y * 10));

        double miny = yy * 40.0 / 60 + y * 5.0 / 60;
        double maxy = yy * 40.0 / 60 + (y + 1) * 5.0 / 60;

        double minx = xx + 60 + x * 7.5 / 60;
        double maxx = xx + 60 + (x + 1) * 7.5 / 60;

        return new Envelope(minx, maxx, miny, maxy);
    }

    /**
     *
     * @param lng
     * @param lat
     * @param cellSize
     * @return 根据经纬度返回全球唯一的字符串编号 gridNo_col_row
     */
    public static String findCell(double lng, double lat, int cellSize) {
        int gridNo = findGrid(lng, lat);
        Envelope ev = getGridBound(gridNo);

        double minx = ev.getMinX();
        double miny = ev.getMinY();
        double maxx = ev.getMaxX();
        double maxy = ev.getMaxY();

        try{
            double xdelta = calPointDistance(minx, miny, maxx, miny);
            double ydelta = calPointDistance(minx, miny, minx, maxy);

            long x_cell_size = (long)Math.floor(xdelta / cellSize);
            long y_cell_size = (long)Math.floor(ydelta / cellSize);
            double spanx = maxx - minx, spany = maxy - miny;

            double x_cell_delta = spanx / x_cell_size;
            double y_cell_delta = spany / y_cell_size;

            int col = (int) ((lng - minx) / x_cell_delta);
            int row = (int) ((lat - miny) / y_cell_delta);

            return gridNo + "_" + col + "_" + row;

        }catch (Exception e){
            return "";
        }
    }

    public static String findCell(double lng,double lat){
        return findCell(lng,lat,DEFAULTSIZE);
    }




    /**
     * 两点之间的球面距离
     *
     * @param sp
     * @param ep
     * @return 计算两个坐标间的球面距离
     */
    public static double calPointDistance(Coordinate sp, Coordinate ep) {
        return calPointDistance(sp.x, sp.y, ep.x, ep.y);
    }

    /**
     * 两点之间的球面距离
     *
     * @param lng1
     * @param lat1
     * @param lng2
     * @param lat2
     * @return 计算两个坐标间的球面距离
     */
    public static double calPointDistance(double lng1, double lat1,
                                          double lng2, double lat2) {
        GeodeticCalculator az = new GeodeticCalculator(DefaultEllipsoid.WGS84);
        az.setStartingGeographicPoint(lng1, lat1);
        az.setDestinationGeographicPoint(lng2, lat2);
        return az.getOrthodromicDistance();
    }


    /**
     * 点到线段的最近点
     *
     * @param p
     * @param l
     * @return
     */
    public static Coordinate closestPoint2LineSegment(Coordinate p,
                                                      LineSegment l) {
        return l.closestPoint(p);
    }



}
