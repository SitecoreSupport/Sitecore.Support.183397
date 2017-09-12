using System;
using System.Collections.Concurrent;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data.LanguageFallback;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    public class LuceneDocumentBuilder : Sitecore.ContentSearch.LuceneProvider.LuceneDocumentBuilder
    {
        public LuceneDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
        {
        }

        public override void AddItemFields()
        {
            try
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields start");
                if (this.Options.IndexAllFields)
                {
                    this.Indexable.LoadAllFields();
                }
                if (this.IsParallel)
                {
                    ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
                    this.ParallelForeachProxy.ForEach<IIndexableDataField>(this.Indexable.Fields, this.ParallelOptions, delegate (IIndexableDataField f) {
                        try
                        {
                            this.CheckAndAddField(this.Indexable, f);
                        }
                        catch (Exception exception)
                        {
                            exceptions.Enqueue(exception);
                        }
                    });
                    if (exceptions.Count > 0)
                    {
                        throw new AggregateException(exceptions);
                    }
                }
                else
                {
                    foreach (IIndexableDataField field in this.Indexable.Fields)
                    {
                        this.CheckAndAddField(this.Indexable, field);
                    }
                }
            }
            finally
            {
                VerboseLogging.CrawlingLogDebug(() => "AddItemFields End");
            }
        }


        private void CheckAndAddField(IIndexable indexable, IIndexableDataField field)
        {
            string name = field.Name;
            if ((this.IsTemplate && this.Options.HasExcludedTemplateFields) && (this.Options.ExcludedTemplateFields.Contains(name) || this.Options.ExcludedTemplateFields.Contains(field.Id.ToString())))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else if ((this.IsMedia && this.Options.HasExcludedMediaFields) && this.Options.ExcludedMediaFields.Contains(field.Name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Media field was excluded.");
            }
            else if (this.Options.ExcludedFields.Contains(field.Id.ToString()) || this.Options.ExcludedFields.Contains(name))
            {
                VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was excluded.");
            }
            else
            {
                try
                {
                    LanguageFallbackFieldSwitcher switcher;
                    if (this.Options.IndexAllFields)
                    {
                        using (switcher = new LanguageFallbackFieldSwitcher(new bool?(this.Index.EnableFieldLanguageFallback)))
                        {
                            this.AddField(field);
                            return;
                        }
                    }
                    if (this.Options.IncludedFields.Contains(name) || this.Options.IncludedFields.Contains(field.Id.ToString()))
                    {
                        using (switcher = new LanguageFallbackFieldSwitcher(new bool?(this.Index.EnableFieldLanguageFallback)))
                        {
                            this.AddField(field);
                            return;
                        }
                    }
                    VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Field was not included.");
                }
                catch (Exception exception)
                {
                    if (this.Settings.StopOnCrawlFieldError())
                    {
                        throw;
                    }
                    CrawlingLog.Log.Fatal(string.Format("Could not add field {1} : {2} for indexable {0}", indexable.UniqueId, field.Id, field.Name), exception);
                }
            }
        }

    }
}