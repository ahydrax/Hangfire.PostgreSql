using System;
using System.Globalization;

namespace Hangfire.PostgreSql
{
    internal static class JobId
    {
        private static readonly NumberFormatInfo InvariantInfo = NumberFormatInfo.InvariantInfo;
        public static long ToLong(string id) => Convert.ToInt64(id, InvariantInfo);
        public static string ToString(long id) => Convert.ToString(id, InvariantInfo);
    }
}
