import {
  IAssetRepository,
  IBaseJob,
  IEntityJob,
  IGeocodingRepository,
  IJobRepository,
  JobName,
  JOBS_ASSET_PAGINATION_SIZE,
  QueueName,
  usePagination,
  WithoutProperty,
} from '@app/domain';
import { AssetEntity, AssetType, ExifEntity } from '@app/infra/entities';
import { Inject, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { ExifDateTime, exiftool, Tags } from 'exiftool-vendored';
import { Duration } from 'luxon';
import fs from 'node:fs';
import { Repository } from 'typeorm/repository/Repository';

interface ImmichTags extends Tags {
  ContentIdentifier?: string;
}

export class MetadataExtractionProcessor {
  private logger = new Logger(MetadataExtractionProcessor.name);
  private reverseGeocodingEnabled: boolean;

  constructor(
    @Inject(IAssetRepository) private assetRepository: IAssetRepository,
    @Inject(IJobRepository) private jobRepository: IJobRepository,
    @Inject(IGeocodingRepository) private geocodingRepository: IGeocodingRepository,
    @InjectRepository(ExifEntity) private exifRepository: Repository<ExifEntity>,

    configService: ConfigService,
  ) {
    this.reverseGeocodingEnabled = !configService.get('DISABLE_REVERSE_GEOCODING');
  }

  async init(deleteCache = false) {
    this.logger.warn(`Reverse geocoding is ${this.reverseGeocodingEnabled ? 'enabled' : 'disabled'}`);
    if (!this.reverseGeocodingEnabled) {
      return;
    }

    try {
      if (deleteCache) {
        await this.geocodingRepository.deleteCache();
      }
      this.logger.log('Initializing Reverse Geocoding');

      await this.jobRepository.pause(QueueName.METADATA_EXTRACTION);
      await this.geocodingRepository.init();
      await this.jobRepository.resume(QueueName.METADATA_EXTRACTION);

      this.logger.log('Reverse Geocoding Initialized');
    } catch (error: any) {
      this.logger.error(`Unable to initialize reverse geocoding: ${error}`, error?.stack);
    }
  }

  async handleQueueMetadataExtraction(job: IBaseJob) {
    const { force } = job;
    const assetPagination = usePagination(JOBS_ASSET_PAGINATION_SIZE, (pagination) => {
      return force
        ? this.assetRepository.getAll(pagination)
        : this.assetRepository.getWithout(pagination, WithoutProperty.EXIF);
    });

    for await (const assets of assetPagination) {
      for (const asset of assets) {
        await this.jobRepository.queue({ name: JobName.METADATA_EXTRACTION, data: { id: asset.id } });
      }
    }

    return true;
  }

  async handleMetadataExtraction({ id }: IEntityJob) {
    const [asset] = await this.assetRepository.getByIds([id]);
    if (!asset || !asset.isVisible) {
      return false;
    }

    switch (asset.type) {
      case AssetType.VIDEO:
        return await this.handleVideoMetadataExtraction(asset);
      case AssetType.IMAGE:
        return await this.handleImageMetadataExtraction(asset);
    }
    return false;
  }

  private async handleImageMetadataExtraction(asset: AssetEntity) {
    const [exifData, tags] = await this.exifData(asset);

    exifData.livePhotoCID = tags.MediaGroupUUID ?? null;

    if (exifData.livePhotoCID && !asset.livePhotoVideoId) {
      const motionAsset = await this.assetRepository.findLivePhotoMatch({
        livePhotoCID: exifData.livePhotoCID,
        otherAssetId: asset.id,
        ownerId: asset.ownerId,
        type: AssetType.VIDEO,
      });
      if (motionAsset) {
        await this.assetRepository.save({ id: asset.id, livePhotoVideoId: motionAsset.id });
        await this.assetRepository.save({ id: motionAsset.id, isVisible: false });
      }
    }

    await this.applyReverseGeocoding(asset, exifData);

    await this.exifRepository.upsert(exifData, { conflictPaths: ['assetId'] });
    await this.assetRepository.save({ id: asset.id, fileCreatedAt: exifData.dateTimeOriginal ?? undefined });

    return true;
  }

  private async handleVideoMetadataExtraction(asset: AssetEntity) {
    const [exifData, tags] = await this.exifData(asset);

    exifData.livePhotoCID = tags.ContentIdentifier ?? null;

    if (exifData.livePhotoCID) {
      const imageAsset = await this.assetRepository.findLivePhotoMatch({
        livePhotoCID: exifData.livePhotoCID,
        ownerId: asset.ownerId,
        otherAssetId: asset.id,
        type: AssetType.IMAGE,
      });
      if (imageAsset) {
        await this.assetRepository.save({ id: imageAsset.id, livePhotoVideoId: asset.id });
        await this.assetRepository.save({ id: asset.id, isVisible: false });
      }
    }

    await this.applyReverseGeocoding(asset, exifData);

    await this.exifRepository.upsert(exifData, { conflictPaths: ['assetId'] });
    await this.assetRepository.save({
      id: asset.id,
      duration: Duration.fromObject({ seconds: tags.Duration }).toFormat('hh:mm:ss.SSS'),
      fileCreatedAt: exifData.dateTimeOriginal ?? undefined,
    });

    return true;
  }

  private async applyReverseGeocoding(asset: AssetEntity, exifData: ExifEntity): Promise<ExifEntity> {
    const { latitude, longitude } = exifData;
    if (!this.reverseGeocodingEnabled || !longitude || !latitude) {
      return exifData;
    }

    try {
      const geocode = await this.geocodingRepository.reverseGeocode({ latitude, longitude });
      return { ...exifData, ...geocode };
    } catch (error: any) {
      this.logger.warn(
        `Unable to run reverse geocoding due to ${error} for asset ${asset.id} at ${asset.originalPath}`,
        error?.stack,
      );
    }
    return exifData;
  }

  private async exifData(asset: AssetEntity): Promise<[ExifEntity, ImmichTags]> {
    const mediaTags = await exiftool.read<ImmichTags>(asset.originalPath).catch((error: any) => {
      this.logger.warn(`error reading exif data (${asset.id} at ${asset.originalPath}): ${error}`, error?.stack);
      return null;
    });

    const sidecarTags = asset.sidecarPath
      ? await exiftool.read<ImmichTags>(asset.sidecarPath).catch((error: any) => {
          this.logger.warn(`error reading exif data (${asset.id} at ${asset.sidecarPath}): ${error}`, error?.stack);
          return null;
        })
      : null;

    const stats = fs.statSync(asset.originalPath);

    const tags = { ...mediaTags, ...sidecarTags };

    const exifDate = (dt: ExifDateTime | string | undefined) => (dt instanceof ExifDateTime ? dt?.toDate() : null);
    const zone = (dt: ExifDateTime | string | undefined) => (dt instanceof ExifDateTime ? dt.zone : null);

    const parseFocalLength = (focalLength: string | undefined) =>
      focalLength ? parseInt(focalLength.split(' ')[0]) : null;

    const validate = <T>(value: T): T | null => (typeof value === 'string' ? null : value ?? null);

    return [
      <ExifEntity>{
        // altitude: tags.GPSAltitude ?? null,
        assetId: asset.id,
        dateTimeOriginal: exifDate(tags.CreateDate) ?? asset.fileCreatedAt,
        exifImageHeight: validate(tags.ImageHeight),
        exifImageWidth: validate(tags.ImageWidth),
        exposureTime: tags.ExposureTime ?? null,
        fileSizeInByte: stats.size,
        fNumber: validate(tags.FNumber),
        focalLength: parseFocalLength(tags.FocalLength),
        fps: validate(tags.VideoFrameRate),
        iso: validate(tags.ISO),
        latitude: validate(tags.GPSLatitude),
        lensModel: tags.LensModel ?? null,
        longitude: validate(tags.GPSLongitude),
        make: tags.Make ?? null,
        model: tags.Model ?? null,
        modifyDate: exifDate(tags.ModifyDate) ?? asset.fileModifiedAt,
        orientation: validate(tags.Orientation)?.toString() ?? null,
        timeZone: zone(tags.OriginalCreateDateTime),
      },
      tags,
    ];
  }
}
