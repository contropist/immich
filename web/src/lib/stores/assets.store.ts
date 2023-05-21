import { AssetGridOptions, AssetGridState } from '$lib/models/asset-grid-state';
import { calculateViewportHeightByNumberOfAsset } from '$lib/utils/viewport-utils';
import { api, TimeBucketResponseDto } from '@api';
import { flatMap, sumBy } from 'lodash-es';
import { derived, writable } from 'svelte/store';

/**
 * The state that holds information about the asset grid
 */
export const assetGridState = writable<AssetGridState>(new AssetGridState());
export const loadingBucketState = writable<{ [key: string]: boolean }>({});
export const assetGridEmpty = derived(
	assetGridState,
	(state) => state.initialized && state.buckets.length === 0
);

function createAssetStore() {
	let _assetGridState = new AssetGridState();
	assetGridState.subscribe((state) => (_assetGridState = state));

	const reset = () => {
		for (const bucket of _assetGridState.buckets) {
			bucket.cancelToken.abort();
		}

		assetGridState.set(new AssetGridState());
	};

	/**
	 * Set initial state
	 * @param viewportHeight
	 * @param viewportWidth
	 * @param data
	 */
	const setInitialState = (
		viewportHeight: number,
		viewportWidth: number,
		timeBuckets: TimeBucketResponseDto[],
		options: AssetGridOptions
	) => {
		assetGridState.set({
			initialized: true,
			viewportHeight,
			viewportWidth,
			timelineHeight: 0,
			buckets: timeBuckets.map(({ timeBucket, count }) => ({
				bucketDate: timeBucket,
				bucketHeight: calculateViewportHeightByNumberOfAsset(count, viewportWidth),
				assets: [],
				cancelToken: new AbortController()
			})),
			assets: [],
			options
		});

		// Update timeline height based on calculated bucket height
		assetGridState.update((state) => {
			state.timelineHeight = sumBy(state.buckets, (d) => d.bucketHeight);
			return state;
		});
	};

	const getAssetsByBucket = async (bucket: string) => {
		try {
			const currentBucketData = _assetGridState.buckets.find((b) => b.bucketDate === bucket);
			if (currentBucketData?.assets && currentBucketData.assets.length > 0) {
				return;
			}
			const { data: assets } = await api.timeBucketApi.getByTimeBucket(
				_assetGridState.options.size,
				bucket,
				...api.getTimeBucketOptions(_assetGridState.options),
				{ signal: currentBucketData?.cancelToken.signal }
			);

			// Update assetGridState with assets by time bucket
			assetGridState.update((state) => {
				const bucketIndex = state.buckets.findIndex((b) => b.bucketDate === bucket);
				state.buckets[bucketIndex].assets = assets;
				state.assets = flatMap(state.buckets, (b) => b.assets);

				return state;
			});
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
		} catch (e: any) {
			if (e.name === 'CanceledError') {
				return;
			}
			console.error('Failed to get asset for bucket ', bucket);
			console.error(e);
		}
	};

	const removeAsset = (assetId: string) => {
		assetGridState.update((state) => {
			const bucketIndex = state.buckets.findIndex((b) => b.assets.some((a) => a.id === assetId));
			const assetIndex = state.buckets[bucketIndex].assets.findIndex((a) => a.id === assetId);
			state.buckets[bucketIndex].assets.splice(assetIndex, 1);

			if (state.buckets[bucketIndex].assets.length === 0) {
				_removeBucket(state.buckets[bucketIndex].bucketDate);
			}
			state.assets = flatMap(state.buckets, (b) => b.assets);
			return state;
		});
	};

	const _removeBucket = (bucketDate: string) => {
		assetGridState.update((state) => {
			const bucketIndex = state.buckets.findIndex((b) => b.bucketDate === bucketDate);
			state.buckets.splice(bucketIndex, 1);
			state.assets = flatMap(state.buckets, (b) => b.assets);
			return state;
		});
	};

	const updateBucketHeight = (bucket: string, actualBucketHeight: number) => {
		assetGridState.update((state) => {
			const bucketIndex = state.buckets.findIndex((b) => b.bucketDate === bucket);
			// Update timeline height based on the new bucket height
			const estimateBucketHeight = state.buckets[bucketIndex].bucketHeight;

			if (actualBucketHeight >= estimateBucketHeight) {
				state.timelineHeight += actualBucketHeight - estimateBucketHeight;
			} else {
				state.timelineHeight -= estimateBucketHeight - actualBucketHeight;
			}

			state.buckets[bucketIndex].bucketHeight = actualBucketHeight;
			return state;
		});
	};

	const cancelBucketRequest = async (token: AbortController, bucketDate: string) => {
		token.abort();
		// set new abort controller for bucket
		assetGridState.update((state) => {
			const bucketIndex = state.buckets.findIndex((b) => b.bucketDate === bucketDate);
			state.buckets[bucketIndex].cancelToken = new AbortController();
			return state;
		});
	};

	const updateAsset = (assetId: string, isFavorite: boolean) => {
		assetGridState.update((state) => {
			const bucketIndex = state.buckets.findIndex((b) => b.assets.some((a) => a.id === assetId));
			const assetIndex = state.buckets[bucketIndex].assets.findIndex((a) => a.id === assetId);
			state.buckets[bucketIndex].assets[assetIndex].isFavorite = isFavorite;

			state.assets = flatMap(state.buckets, (b) => b.assets);
			return state;
		});
	};

	return {
		reset,
		setInitialState,
		getAssetsByBucket,
		removeAsset,
		updateBucketHeight,
		cancelBucketRequest,
		updateAsset
	};
}

export const assetStore = createAssetStore();
