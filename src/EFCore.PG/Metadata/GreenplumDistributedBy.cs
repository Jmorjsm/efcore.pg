using System;
using System.Collections.Generic;
using System.Text;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql.EntityFrameworkCore.PostgreSQL.Extensions.MetadataExtensions;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using Npgsql.EntityFrameworkCore.PostgreSQL.Utilities;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Metadata
{
    public class GreenplumDistributedBy
    {
        const string AnnotationName = GreenplumDistributeByAnnotationNames.DistributeBy;

        readonly IAnnotatable _annotatable;

        public virtual Annotatable Annotatable => (Annotatable)_annotatable;

        public GreenplumDistributedBy([NotNull] IAnnotatable annotatable)
            => _annotatable = annotatable;

        public virtual List<string> DistributeByColumnNames
        {
            get => GetData();
            [param: NotNull] set
            {
                SetData(value);
            }
        }

        public NpgsqlGreenplumDistribution DistributionType { get; set; }

        List<string> GetData()
        {
            var str =  Annotatable[AnnotationName] as string;
            return str == null ? null : Deserialize(str);
        }

        void SetData(List<string> distributeByColumnNames)
            => Annotatable[AnnotationName] = Serialize(distributeByColumnNames);

        static List<string> Deserialize([NotNull] string value)
        {
            Check.NotEmpty(value, nameof(value));

            try
            {
                var position = 0;
                var columnNames = new List<string>();
                while (position < value.Length)
                {
                    // Find the first quote.
                    position = value.IndexOf('\'', position) + 1;
                    // Start looking for the end quote.
                    var endPosition = value.IndexOf('\'', position);
                    // Continue while quotes are paired.
                    while (endPosition + 1 < value.Length && value[endPosition + 1] == '\'')
                    {
                        // Check if the next quote is paired.
                        endPosition = value.IndexOf('\'', endPosition + 2);
                    }

                    columnNames.Add(value.Substring(position, endPosition - position).Replace("''","'"));
                    // Start looking for the next starting quote after the end of this one.
                    position = endPosition + 1;
                }

                return columnNames;
            }
            catch (Exception ex)
            {
                throw new ArgumentException($"Couldn't deserialize {nameof(GreenplumDistributedBy)} from annotation", ex);
            }
        }

        internal static string Serialize(List<string> distributeByColumns)
        {
            Check.HasNoNulls(distributeByColumns, nameof(distributeByColumns));
            StringBuilder builder = new StringBuilder();

            for (var index = 0; index < distributeByColumns.Count; index++)
            {
                var col = distributeByColumns[index];
                EscapeAndQuote(builder, col);
                if(index < distributeByColumns.Count-1)
                    builder.Append(", ");
            }

            return builder.ToString();
        }

        static void EscapeAndQuote(StringBuilder stringBuilder, string col)
        {
            stringBuilder.Append('\'');
            stringBuilder.Append(col.Replace("'","''"));
            stringBuilder.Append('\'');
        }
    }
}
