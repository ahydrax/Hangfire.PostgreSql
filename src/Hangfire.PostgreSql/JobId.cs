using System;
using System.Globalization;

namespace Hangfire.PostgreSql
{
    internal static class JobId
    {
        public static long ToLong(string id) => Convert.ToInt64(id, NumberFormatInfo.InvariantInfo);
        public static string ToString(long id) => Convert.ToString(id, NumberFormatInfo.InvariantInfo);
    }
}
