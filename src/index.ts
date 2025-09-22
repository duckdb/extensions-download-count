import "../worker-configuration.d.ts"

import { WorkerEntrypoint } from "cloudflare:workers";

// https://stackoverflow.com/a/6117889
Date.prototype.getWeekNumber = function(){
	var d = new Date(Date.UTC(this.getFullYear(), this.getMonth(), this.getDate()));
	var dayNum = d.getUTCDay() || 7;
	d.setUTCDate(d.getUTCDate() + 4 - dayNum);
	var yearStart = new Date(Date.UTC(d.getUTCFullYear(),0,1));
	return Math.ceil((((d - yearStart) / 86400000) + 1)/7)
};

export default class extends WorkerEntrypoint<Env> {
	async scheduled(
		controller: ScheduledController,
		env: Env,
		ctx: ExecutionContext,
		) {

		const cloudflare_api_key = this.env.CF_API_KEY;
		const cloudflare_zone_id = '84f631c38b77d4631b561207f2477332';

		const url = 'https://api.cloudflare.com/client/v4/graphql';
	const host = this.env.DUCKDB_HOSTNAME;
	const r2_bucket = host.replace(/[-.]/g, '_');

	console.log(host)

	const headers = {
		'User-Agent':           host,
		'Authorization':        'Bearer ' +  cloudflare_api_key,
		'Accept':               'application/json',
		'X-GitHub-Api-Version': '2022-11-28'
	};

	let last_week = new Date();
    last_week.setDate(last_week.getDate() - 7); // 7 days is all we get from cf.

    const release_version = await (await fetch('https://duckdb.org/data/latest_stable_version.txt')).text();

    const options = {
    	limit: 1000,
    	prefix: "v"+release_version.replace('\n', '')+"/linux_arm64",
    };


    const list = await this.env[r2_bucket].list(options);

    const extensions = list.objects.map((key) => key.key.replace('.duckdb_extension.gz', '').split('/').pop());

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
    		"datetime_geq": "`+last_week.toISOString()+`",
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

    const today = new Date();
    extension_counts['_last_update'] = today.toISOString()

    console.log(extension_counts);
    await this.env[r2_bucket].put('downloads-last-week.json', JSON.stringify(extension_counts), {
    	httpMetadata: {contentType : 'application/json'}
    });

    const year_week = today.getFullYear() + '/' + today.getWeekNumber();
    await this.env[r2_bucket].put('download-stats-weekly/'+year_week+'.json', JSON.stringify(extension_counts), {
    	httpMetadata: {contentType : 'application/json'}
    });
  }
};
