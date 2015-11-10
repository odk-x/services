/*
 * Copyright (C) 2012 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.common.android.utilities;

/**
 * Class for converting UTM coordinates into latitude/longitude
 * coordinates.
 * 
 * @author Charles L. Taylor
 * @author Melissa Winstanley
 * @version Copyright 1997-1998
 * @version http://home.hiwaay.net/~taylorc/toolbox/geography/geoutm.html
 */
public class UTMConverter {
    
    /** Ellipsoid model constants (actual values here are for WGS84) */
    private static final double SM_A = 6378137.0;
    private static final double SM_B = 6356752.314;
    private static final double UTM_SCALE_FACTOR = 0.9996;


    /**
     * Converts degrees to radians.
     */
    private static double degToRad(double deg) {
        return deg / 180.0 * Math.PI;
    }

    /**
     * Converts radians to degrees.
     */
    private static double radToDeg(double rad) {
        return rad / Math.PI * 180.0;
    }

    /**
     * Determines the central meridian for the given UTM zone.
     *
     * @param zone value designating the UTM zone, range [1,60].
     *
     * @return the central meridian for the given UTM zone, in radians, or zero
     *   if the UTM zone parameter is outside the range [1,60].
     *   Range of the central meridian is the radian equivalent of [-177,+177].
     *
     */
    private static double UTMCentralMeridian(int zone) {
        return degToRad(-183.0 + zone * 6.0);
    }

    /**
     * Computes the footpoint latitude for use in converting transverse
     * Mercator coordinates to ellipsoidal coordinates.
     *
     * Reference: Hoffmann-Wellenhof, B., Lichtenegger, H., and Collins, J.,
     *   GPS: Theory and Practice, 3rd ed.  New York: Springer-Verlag Wien, 1994.
     *
     * @param y the UTM northing coordinate, in meters
     *
     * @return the footpoint latitude, in radians
     */
    private static double footpointLatitude(double y) {
        /* Precalculate n (Eq. 10.18) */
        double n = (SM_A - SM_B) / (SM_A + SM_B);

        /* Precalculate alpha_ (Eq. 10.22)
           (Same as alpha in Eq. 10.17) */
        double alpha_ = ((SM_A + SM_B) / 2.0)
                         * (1 + (Math.pow(n, 2.0) / 4) +
                                (Math.pow(n, 4.0) / 64));

        /* Precalculate y_ (Eq. 10.23) */
        double y_ = y / alpha_;

        /* Precalculate beta_ (Eq. 10.22) */
        double beta_ = (3.0 * n / 2.0) + (-27.0 * Math.pow(n, 3.0) / 32.0)
                                       + (269.0 * Math.pow(n, 5.0) / 512.0);

        /* Precalculate gamma_ (Eq. 10.22) */
        double gamma_ = (21.0 * Math.pow(n, 2.0) / 16.0)
                         + (-55.0 * Math.pow(n, 4.0) / 32.0);

        /* Precalculate delta_ (Eq. 10.22) */
        double delta_ = (151.0 * Math.pow(n, 3.0) / 96.0)
                         + (-417.0 * Math.pow(n, 5.0) / 128.0);

        /* Precalculate epsilon_ (Eq. 10.22) */
        double epsilon_ = (1097.0 * Math.pow(n, 4.0) / 512.0);
  
        /* Now calculate the sum of the series (Eq. 10.21) */
        double result = y_ + (beta_ * Math.sin(2.0 * y_))
                           + (gamma_ * Math.sin(4.0 * y_))
                           + (delta_ * Math.sin(6.0 * y_))
                           + (epsilon_ * Math.sin(8.0 * y_));
        return result;
    }

    /**
     * Converts x and y coordinates in the Transverse Mercator projection to
     * a latitude/longitude pair.  Note that Transverse Mercator is not
     * the same as UTM; a scale factor is required to convert between them.
     *
     * Reference: Hoffmann-Wellenhof, B., Lichtenegger, H., and Collins, J.,
     *   GPS: Theory and Practice, 3rd ed.  New York: Springer-Verlag Wien, 1994.
     *
     * @param x the easting of the point, in meters.
     * @param y the northing of the point, in meters.
     * @param lambda0 longitude of the central meridian to be used, in radians.
     * @param philambda a 2-element to store the latitude and longitude
     *                  in radians.
     */
    private static void mapXYToLatLon(double x, double y, double lambda0,
                                      double[] philambda) {
        /*
         * Remarks:
         *   The local variables Nf, nuf2, tf, and tf2 serve the same purpose as
         *   N, nu2, t, and t2 in MapLatLonToXY, but they are computed with respect
         *   to the footpoint latitude phif.
         *
         *   x1frac, x2frac, x2poly, x3poly, etc. are to enhance readability and
         *   to optimize computations.
         */

        /* Get the value of phif, the footpoint latitude. */
        double phif = footpointLatitude (y);

        /* Precalculate ep2 */
        double ep2 = (Math.pow(SM_A, 2.0) - Math.pow(SM_B, 2.0))
                     / Math.pow(SM_B, 2.0);

        /* Precalculate cos (phif) */
        double cf = Math.cos(phif);

        /* Precalculate nuf2 */
        double nuf2 = ep2 * Math.pow(cf, 2.0);

        /* Precalculate Nf and initialize Nfpow */
        double Nf = Math.pow(SM_A, 2.0) / (SM_B * Math.sqrt(1 + nuf2));
        double Nfpow = Nf;

        /* Precalculate tf */
        double tf = Math.tan(phif);
        double tf2 = tf * tf;
        double tf4 = tf2 * tf2;

        /* Precalculate fractional coefficients for x**n in the equations
           below to simplify the expressions for latitude and longitude. */
        double x1frac = 1.0 / (Nfpow * cf);

        Nfpow *= Nf;   /* now equals Nf**2) */
        double x2frac = tf / (2.0 * Nfpow);
      
        Nfpow *= Nf;   /* now equals Nf**3) */
        double x3frac = 1.0 / (6.0 * Nfpow * cf);
      
        Nfpow *= Nf;   /* now equals Nf**4) */
        double x4frac = tf / (24.0 * Nfpow);
      
        Nfpow *= Nf;   /* now equals Nf**5) */
        double x5frac = 1.0 / (120.0 * Nfpow * cf);
      
        Nfpow *= Nf;   /* now equals Nf**6) */
        double x6frac = tf / (720.0 * Nfpow);
      
        Nfpow *= Nf;   /* now equals Nf**7) */
        double x7frac = 1.0 / (5040.0 * Nfpow * cf);
      
        Nfpow *= Nf;   /* now equals Nf**8) */
        double x8frac = tf / (40320.0 * Nfpow);
      
        /* Precalculate polynomial coefficients for x**n.
           -- x**1 does not have a polynomial coefficient. */
        double x2poly = -1.0 - nuf2;
      
        double x3poly = -1.0 - 2 * tf2 - nuf2;
      
        double x4poly = 5.0 + 3.0 * tf2 + 6.0 * nuf2 - 6.0 * tf2 * nuf2
                            - 3.0 * (nuf2 *nuf2) - 9.0 * tf2 * (nuf2 * nuf2);
      
        double x5poly = 5.0 + 28.0 * tf2 + 24.0 * tf4 + 6.0 * nuf2 + 8.0 * tf2 * nuf2;
      
        double x6poly = -61.0 - 90.0 * tf2 - 45.0 * tf4 - 107.0 * nuf2
                              + 162.0 * tf2 * nuf2;
      
        double x7poly = -61.0 - 662.0 * tf2 - 1320.0 * tf4 - 720.0 * (tf4 * tf2);
      
        double x8poly = 1385.0 + 3633.0 * tf2 + 4095.0 * tf4 + 1575 * (tf4 * tf2);
          
        /* Calculate latitude */
        philambda[0] = phif + x2frac * x2poly * (x * x)
                            + x4frac * x4poly * Math.pow(x, 4.0)
                            + x6frac * x6poly * Math.pow(x, 6.0)
                            + x8frac * x8poly * Math.pow(x, 8.0);
          
        /* Calculate longitude */
        philambda[1] = lambda0 + x1frac * x
                               + x3frac * x3poly * Math.pow(x, 3.0)
                               + x5frac * x5poly * Math.pow(x, 5.0)
                               + x7frac * x7poly * Math.pow(x, 7.0);

    }

    /**
     * Converts x and y coordinates in the Universal Transverse Mercator
     * projection to a latitude/longitude pair.
     *
     * @param x the easting of the point, in meters.
     * @param y the northing of the point, in meters.
     * @param zone the UTM zone in which the point lies.
     * @param southhemi true if the point is in the southern hemisphere;
     *                  false otherwise.
     * @param latlon a 2-element array to store the latitude and
     *               longitude of the point, in radians.
     */
    private static void UTMXYToLatLon(double x, double y, int zone, boolean southhemi,
                                      double[] latlon) {
        x -= 500000.0;
        x /= UTM_SCALE_FACTOR;

        /* If in southern hemisphere, adjust y accordingly. */
        if (southhemi) {
            y -= 10000000.0;
        }
        
        y /= UTM_SCALE_FACTOR;

        double cmeridian = UTMCentralMeridian(zone);
        mapXYToLatLon(x, y, cmeridian, latlon);
    }

    /**
     * Converts the given UTM coordinates to a latitude/longitude pair.
     * 
     * @param x the easting of the point, in meters.
     * @param y the northing of the point, in meters.
     * @param zone the UTM zone in which the point lies.
     * @param southhemi true if the point is in the southern hemisphere;
     *                  false otherwise.
     * @return a 2-element array storing the latitude and longitude of the given UTM
     *         point, or null if the given UTM point is invalid.
     */
    public static double[] parseUTM(double x, double y, int zone, boolean southhemi) {
        if (x < 0 || y < 0 || zone < 1 || zone > 60) {
            return null;
        }
        double[] latlon = new double[2];

        UTMXYToLatLon(x, y, zone, southhemi, latlon);
        latlon[0] = radToDeg(latlon[0]);
        latlon[1] = radToDeg(latlon[1]);
        if (latlon[0] > -90 &&  latlon[0] < 90 && latlon[1] > -180 && latlon[1] < 180) {
            return latlon;
        } else {
            return null;
        }
    }
}
