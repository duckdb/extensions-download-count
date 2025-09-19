interface Env {}
export default {
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ) {

	const cloudflare_api_key = env.CF_API_KEY;
	const cloudflare_zone_id = '84f631c38b77d4631b561207f2477332';

	const url = 'https://api.cloudflare.com/client/v4/graphql';
	const host = 'extensions.duckdb.org'; // TODO

	const headers = {
		'User-Agent':           'extensions.duckdb.org',
		'Authorization':        'Bearer ' +  cloudflare_api_key,
		'Accept':               'application/json',
		'X-GitHub-Api-Version': '2022-11-28'
	};

    let date = new Date();
    date.setDate(date.getDate() - 7); // 7 days is all we get from cf.

	const extensions = ['autocomplete', 'avro', 'aws', 'azure', 'delta', 'ducklake', 'encodings', 'excel', 'fts', 'httpfs', 'iceberg', 'icu', 'inet', 'jemalloc', 'mysql_scanner', 'postgres_scanner', 'spatial', 'sqlite_scanner', 'tpcds', 'tpch', 'ui', 'vss', 'motherduck', 'json', 'parquet'];

	const http_requests = extensions.map((ext) => {
		const graphql = `
		    { "query":
		      "query ExtensionsDownloadsLastWeek($zoneTag: string, $filter:filter) {
		        viewer {
		          zones(filter: {zoneTag: $zoneTag}) {
		            httpRequestsAdaptiveGroups(limit: 10000, filter: $filter) {
		              count
		            }
		          }
		        }
		      }",
		      "variables": {
		        "zoneTag": "`+cloudflare_zone_id+`",
		        "filter": {
		          "datetime_geq": "`+date.toISOString()+`",
		          "clientRequestHTTPHost": "`+host+`",
		          "edgeResponseStatus": 200,
		          "clientRequestPath_like": "%%/`+ext+`.duckdb_extension.%%"
		        }
		      }
		    }`;
		return fetch(url, {
			'method':  'POST', 
			'headers': headers, 
			'body':    graphql.replace(/(\n|\t)/g, '')});
	});

	const http_request_results = await Promise.all(http_requests);

	var extension_counts = {};
	for (var idx in extensions) {
		const res_json = await http_request_results[idx].json();
		var count = 0;
		try {
			count = res_json.data.viewer.zones[0].httpRequestsAdaptiveGroups[0].count;
		} catch {}
		extension_counts[extensions[idx]] = count;
	}

	await this.env.R2.put('downloads-last-week.json', extension_counts, {
      // httpMetadata: request.headers,
    });

  },
};
