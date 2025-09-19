
import { WorkerEntrypoint } from "cloudflare:workers";
declare namespace Cloudflare {
	interface Env {
		R2: R2Bucket;
	}
}
interface Env extends Cloudflare.Env {}

export default class extends WorkerEntrypoint<Env> {
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

	var http_requests = extensions.map((ext) => {
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

	// we can't have more than n outstanding requests on cf
	var counts = [];
	while (http_requests.length > 0) {
		const req_now = http_requests.slice(0, 5);
		http_requests = http_requests.slice(5);
		const res_now = await Promise.all(req_now);

		for (var res of res_now) {
			const res_json = await res.json();
			try {
				counts.push(res_json.data.viewer.zones[0].httpRequestsAdaptiveGroups[0].count);
			} catch {
				counts.push(0);
			}
		}
	}
	if (counts.size != http_requests.size) {
		throw new RangeError();
	}

	var extension_counts = {};
	for (var idx in extensions) {
		extension_counts[extensions[idx]] = counts[idx];
	}

	console.log(extension_counts);
	console.log(this.env.R2)
	await this.env.R2.put('downloads-last-week.json', JSON.stringify(extension_counts), {
      // httpMetadata: request.headers,
    });

  }
};
