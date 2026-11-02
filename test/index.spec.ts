import { env, fetchMock, SELF } from 'cloudflare:test';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

const repositories = [
	{ host: 'extensions.duckdb.org', bucket: 'extensions_duckdb_org' },
	{ host: 'community-extensions.duckdb.org', bucket: 'community_extensions_duckdb_org' },
] as const;

describe('scheduled download counts', () => {
	beforeEach(() => {
		fetchMock.activate();
		fetchMock.disableNetConnect();
	});

	afterEach(() => {
		fetchMock.assertNoPendingInterceptors();
		fetchMock.deactivate();
	});

	it('publishes GET-only counts to the latest and weekly objects for both repositories', async () => {
		const filters: Record<string, unknown>[] = [];
		for (const { host } of repositories) {
			fetchMock
				.get('https://api.cloudflare.com')
				.intercept({ path: '/client/v4/graphql', method: 'POST' })
				.reply(200, ({ body }) => {
					const { variables } = JSON.parse(body as string);
					const filter = variables.filter;
					filters.push(filter);
					expect(filter).toMatchObject({
						clientRequestHTTPHost: host,
						edgeResponseStatus: 200,
						clientRequestPath_like: '%%.duckdb_extension.%%',
					});
					expect(Number.isNaN(Date.parse(filter.datetime_geq))).toBe(false);

					// Model analytics aggregation: HEAD probes are included unless
					// the outgoing query explicitly selects GET requests.
					const requests = [
						{ method: 'GET', path: '/v1.5.6/linux_amd64/example.duckdb_extension.gz', count: 3 },
						{ method: 'HEAD', path: '/v1.5.6/linux_amd64/example.duckdb_extension.gz', count: 100 },
						{ method: 'GET', path: '/v2.0.0/wasm_eh/example.duckdb_extension.wasm', count: 2 },
					];
					const counts = new Map<string, number>();
					for (const request of requests) {
						if (filter.clientRequestHTTPMethodName && filter.clientRequestHTTPMethodName !== request.method) {
							continue;
						}
						counts.set(request.path, (counts.get(request.path) ?? 0) + request.count);
					}
					return JSON.stringify({
						data: {
							viewer: {
								zones: [{
									httpRequestsAdaptiveGroups: Array.from(counts, ([path, count]) => ({
										count,
										dimensions: { clientRequestPath: path },
									})),
								}],
							},
						},
					});
				});
		}

		await SELF.scheduled();

		for (const { bucket } of repositories) {
			const latest = await env[bucket].get('downloads-last-week.json');
			expect(latest).not.toBeNull();
			expect(latest!.httpMetadata?.contentType).toBe('application/json');
			const counts = await latest!.json();
			expect(counts).toEqual({ example: 5, _last_update: expect.any(String) });

			const archive = await env[bucket].list({ prefix: 'download-stats-weekly/' });
			expect(archive.objects).toHaveLength(1);
			const weekly = await env[bucket].get(archive.objects[0].key);
			expect(await weekly!.json()).toEqual(counts);
		}
		expect(filters).toHaveLength(2);
		for (const filter of filters) {
			expect(filter.clientRequestHTTPMethodName).toBe('GET');
		}
	});
});
