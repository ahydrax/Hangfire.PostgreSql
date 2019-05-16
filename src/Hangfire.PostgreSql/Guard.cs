using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Hangfire.Annotations;
using Npgsql;

namespace Hangfire.PostgreSql
{
    [DebuggerStepThrough]
    internal static class Guard
    {
        private const string HostHasNotFoundExceptionMessage = "Invalid Postgres connection string: Host has not found.";
        private const string SearchPathIsNotSpecified = "Invalid Postgres connection string: Search Path has not found.";

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNull([NotNull] object argument, [NotNull] string argumentName)
        {
            if (argument == null)
                throw new ArgumentNullException(argumentName);
        }

        [ContractAnnotation("condition:true => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIf([NotNull] bool condition, [NotNull] string message)
        {
            if (condition)
                throw new ArgumentException(message);
        }

        [ContractAnnotation("argument:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNullOrEmpty([NotNull] string argument, [NotNull] string argumentName)
        {
            if (string.IsNullOrWhiteSpace(argument))
                throw new ArgumentNullException(argumentName);
        }

        [ContractAnnotation("collection:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfCollectionIsNullOrEmpty<T>(
            [NotNull] IReadOnlyCollection<T> collection,
            [NotNull] string argumentName)
        {
            if (collection == null) throw new ArgumentNullException(argumentName);
            if (collection.Count == 0) throw new ArgumentException($"{argumentName} should be non-empty collection", argumentName);
        }

        [ContractAnnotation("connectionString:null => halt")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfConnectionStringIsInvalid([NotNull] string connectionString)
        {
            ThrowIfNull(connectionString, nameof(connectionString));
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);
            if (connectionStringBuilder.Host == null) throw new ArgumentException(HostHasNotFoundExceptionMessage);
            if (connectionStringBuilder.SearchPath == null) throw new ArgumentException(SearchPathIsNotSpecified);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfValueIsNotPositive(TimeSpan value, [NotNull] string fieldName)
        {
            var message = $"The {fieldName} property value must be positive value. Given: {value}.";

            if (value == TimeSpan.Zero)
            {
                throw new ArgumentException(message, fieldName);
            }
            if (value != value.Duration())
            {
                throw new ArgumentException(message, fieldName);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfValueIsNegative(TimeSpan value, [NotNull] string fieldName)
        {
            var message = $"The {fieldName} property value must be zero or positive value. Given: {value}.";

            if (value != value.Duration())
            {
                throw new ArgumentException(message, fieldName);
            }
        }
    }
}
