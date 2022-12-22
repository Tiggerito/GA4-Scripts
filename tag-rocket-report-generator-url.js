// Tag Rocket Report Generation URL v1.0

// https://developers.google.com/looker-studio/integrate/linking-api


var createReportUrl = function (storeName, projectAndDatasetId, ga4AccountId, ga4PropertyId, options) {
    var version  = "4.0";

    var reportId = "9efae845-66ce-4833-acd3-910a8212ebbb";

    var reportName = "Tag Rocket Report - "+storeName+" - v"+version+" by Web Site Advantage";

    var url = "https://datastudio.google.com/reporting/create?";

    url += "c.reportId="+encodeURIComponent(reportId);

    if (options.explain) {
        url += "&c.explain="+encodeURIComponent(options.explain);
    }

    if (options.pageId) {
        url += "&c.pageId="+encodeURIComponent(options.pageId);
    }

    if (options.mode) {
        url += "&c.mode="+encodeURIComponent(options.mode);
    }

    url += "&r.reportName="+encodeURIComponent(reportName);

    url += createBigQueryDataSourceParameters(storeName, projectAndDatasetId, "purchases");
    url += createBigQueryDataSourceParameters(storeName, projectAndDatasetId, "missing_pages");
    url += createBigQueryDataSourceParameters(storeName, projectAndDatasetId, "website_errors");
    url += createBigQueryDataSourceParameters(storeName, projectAndDatasetId, "web_vitals_summary");

    url += createGA4DataSourceParameters(storeName, "ga4", ga4AccountId, ga4PropertyId);

    return url;
}

var createBigQueryDataSourceParameters = function(storeName, projectAndDatasetId, tableIdAndAlias) {

    var parts = projectAndDatasetId.split(".");

    if (parts.length != 2) throw "invalid projectAndDatasetId. should be in the form project_id.analytics_123456789";

    var parameters = "";

    parameters += "&ds."+tableIdAndAlias+".connector=bigQuery";
    parameters += "&ds."+tableIdAndAlias+".datasourceName="+encodeURIComponent(tableIdAndAlias+" - " + storeName);
    parameters += "&ds."+tableIdAndAlias+".projectId="+encodeURIComponent(parts[0]);
    parameters += "&ds."+tableIdAndAlias+".type=TABLE";
    parameters += "&ds."+tableIdAndAlias+".datasetId="+encodeURIComponent(parts[1]);
    parameters += "&ds."+tableIdAndAlias+".tableId="+encodeURIComponent(tableId);
    parameters += "&ds."+tableIdAndAlias+".isPartitioned=true";

    return parameters;
}

var createGA4DataSourceParameters = function(storeName, alias, ga4AccountId, ga4PropertyId) {

    var parts = projectAndDatasetId.split(".");

    if (parts.length != 2) throw "invalid projectAndDatasetId. should be in the form project_id.analytics_123456789";

    var parameters = "";

    parameters += "&ds."+alias+".connector=googleAnalytics";
    parameters += "&ds."+alias+".datasourceName="+encodeURIComponent(alias+" - " + storeName);
    parameters += "&ds."+alias+".accountId="+encodeURIComponent(ga4AccountId);
    parameters += "&ds."+alias+".propertyId="+encodeURIComponent(ga4PropertyId);

    return parameters;
}
