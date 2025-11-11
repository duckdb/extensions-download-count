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


	const graphql = `
	    { "query":
	      "query ExtensionsDownloadsLastWeek($zoneTag: string, $filter:filter) {
	        viewer {
	          zones(filter: {zoneTag: $zoneTag}) {
	            httpRequestsAdaptiveGroups(limit: 10000, filter: $filter) {
	              count
		           dimensions {
          clientRequestPath
        }
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
      "clientRequestPath_like": "%%.duckdb_extension.%%"
	        }
	      }
	}`;
   	const fetch_res = await (await fetch(url, {
    		'method':  'POST', 
    		'headers': headers, 
    		'body':    graphql.replace(/(\n|\t)/g, '')})).json();
    
    const today = new Date();
    var extension_counts = {};
    extension_counts['_last_update'] = today.toISOString()


   	for (const path of fetch_res.data.viewer.zones[0].httpRequestsAdaptiveGroups) {
   		const matches = path.dimensions.clientRequestPath.match('.*/([^/]+)\.duckdb_extension.*');
   		if (!matches) {
   			continue; // ???
   		}
   		const extension_name = matches[1];
   		if (extension_name in extension_counts) {
   			extension_counts[extension_name] += path.count;
   		} else {
   			extension_counts[extension_name] = path.count;
   		}
   	}

    await this.env[r2_bucket].put('downloads-last-week.json', JSON.stringify(extension_counts), {
    	httpMetadata: {contentType : 'application/json'}
    });

    const year_week = today.getFullYear() + '/' + today.getWeekNumber();
    await this.env[r2_bucket].put('download-stats-weekly/'+year_week+'.json', JSON.stringify(extension_counts), {
    	httpMetadata: {contentType : 'application/json'}
    });
  }
};
