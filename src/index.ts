// 1. Call "/populate" and fill the bucket up with at least 900 R2 objects with
//    about 500 bytes of metadata attached to each.
//
// 2. Request "/" and observe "Error: internal error"
//
// It seems that if I omit the option to include customMetadata, it does not
// fail. But if I include customMetadata I have to set a low `limit` in order
// to avoid the "Error: internal error"

export interface Env {
	R2: R2Bucket;
}

export default {
	async fetch(
		request: Request,
		env: Env,
		_ctx: ExecutionContext
	): Promise<Response> {
		const { pathname } = new URL(request.url);
		if (pathname === '/populate') {
			return populate(request, env);
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
			listResult.objects.forEach(obj => results.push(obj.key));
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

// Populate an R2 bucket with some junk records.
async function populate(
	request: Request,
	env: Env,
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
			},
		});
	}
	return new Response(`Created ${count} sample records.`);
}
