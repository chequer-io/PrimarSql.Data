using Amazon;
using PrimarSql.Data.Models;

namespace PrimarSql.Data.Extensions
{
    internal static class AwsRegionExtension
    {
        public static RegionEndpoint ToRegionEndpoint(this AwsRegion region)
        {
            switch (region)
            {
                case AwsRegion.USEast1:
                    return RegionEndpoint.USEast1;

                case AwsRegion.USEast2:
                    return RegionEndpoint.USEast2;

                case AwsRegion.USWest1:
                    return RegionEndpoint.USWest1;

                case AwsRegion.USWest2:
                    return RegionEndpoint.USWest2;

                case AwsRegion.EUNorth1:
                    return RegionEndpoint.EUNorth1;

                case AwsRegion.EUWest1:
                    return RegionEndpoint.EUWest1;

                case AwsRegion.EUWest2:
                    return RegionEndpoint.EUWest2;

                case AwsRegion.EUWest3:
                    return RegionEndpoint.EUWest3;

                case AwsRegion.EUCentral1:
                    return RegionEndpoint.EUCentral1;

                case AwsRegion.EUSouth1:
                    return RegionEndpoint.EUSouth1;

                case AwsRegion.APEast1:
                    return RegionEndpoint.APEast1;

                case AwsRegion.APNortheast1:
                    return RegionEndpoint.APNortheast1;

                case AwsRegion.APNortheast2:
                    return RegionEndpoint.APNortheast2;

                case AwsRegion.APNortheast3:
                    return RegionEndpoint.APNortheast3;

                case AwsRegion.APSouth1:
                    return RegionEndpoint.APSouth1;

                case AwsRegion.APSoutheast1:
                    return RegionEndpoint.APSoutheast1;

                case AwsRegion.APSoutheast2:
                    return RegionEndpoint.APSoutheast2;

                case AwsRegion.SAEast1:
                    return RegionEndpoint.SAEast1;

                case AwsRegion.USGovCloudEast1:
                    return RegionEndpoint.USGovCloudEast1;

                case AwsRegion.USGovCloudWest1:
                    return RegionEndpoint.USGovCloudWest1;

                case AwsRegion.CNNorth1:
                    return RegionEndpoint.CNNorth1;

                case AwsRegion.CNNorthWest1:
                    return RegionEndpoint.CNNorthWest1;

                case AwsRegion.CACentral1:
                    return RegionEndpoint.CACentral1;

                case AwsRegion.MESouth1:
                    return RegionEndpoint.MESouth1;

                case AwsRegion.AFSouth1:
                    return RegionEndpoint.AFSouth1;
            }

            return null;
        }
    }
}
