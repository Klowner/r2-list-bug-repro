/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `wrangler dev src/index.ts` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `wrangler publish src/index.ts --name my-worker` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */


// 1. Call /populate and fill the bucket up with at least 1000 R2 objects with
//    sizable metadata attached.
//
// 2.

export interface Env {
	R2: R2Bucket;
}

export default {
	async fetch(
		request: Request,
		env: Env,
		ctx: ExecutionContext
	): Promise<Response> {
		const { pathname } = new URL(request.url);
		if (pathname === '/populate') {
			return populate(request, env, ctx);
		}
		let more = true;
		let cursor: string|undefined;
		const results: unknown[] = [];
		while (more) {
			const listResult = await env.R2.list({
				cursor,
				include: ['customMetadata'],
			});
			console.log(listResult.objects.length + ' records returned');
			listResult.objects.forEach(obj => results.push(obj.customMetadata));
			more = listResult.truncated;
			cursor = listResult.cursor;
		}
		return new Response(JSON.stringify(results, null, 2), {
			headers: {
				'Content-Type': 'application/json',
			},
		});
	},
};

async function populate(
	request: Request,
	env: Env,
	ctx: ExecutionContext,
): Promise<Response> {
	const { searchParams } = new URL(request.url);
	const count = parseInt(searchParams.get('count') || '900', 10);
	for (let i = 0; i < count; i++) {
		await env.R2.put(`test${i}`, '-----content------', {
			customMetadata: {
				"state": ("{\"location\":\"0002\",\"metadata\":{\"location\":\"/0002\",\"name\":\"com" +
					".fobleokfeokfpeokfepokf.apk\",\"internal_user_id\":\"123\",\"action\":\"release\"}" +
					",\"uploadLength\":2126219066,\"createParams\":{\"id\":\"/0002\",\"path\":\"/0002\"," +
					"\"contentLength\":0,\"uploadLength\":2126219066,\"uploadMetadata\":{\"name\":\"com" +
					".oksdosapdkaspodkxxxxxxpkcpoxkocpkx.apk\",\"internal_user_id\":\"123\",\"action\":" +
					"\"release\"},\"uploadConcat\":null},\"currentOffset\":1308000000,\"parts\":null}"),
				// "prevKey": "0000/000000000",
			},
		});
	}
	return new Response('CREATED RECORDS');
}
