import { Job, Processor } from "bullmq";
import {
  clickhouseClient,
  getClickhouseEntityType,
  getCurrentSpan,
  getQueue,
  getS3EventStorageClient,
  IngestionEventType,
  logger,
  QueueName,
  recordDistribution,
  recordHistogram,
  recordIncrement,
  redis,
  TQueueJobTypes,
  traceException,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { env } from "../env";
import { IngestionService } from "../services/IngestionService";
import { ClickhouseWriter, TableName } from "../services/ClickhouseWriter";
import { chunk } from "lodash";
import { randomUUID } from "crypto";

import { Buffer } from "buffer";
import { PubSubPublisher } from "../services/PubSubPublisher";

const prodPubSubTopicName = "langfuse-events";
const devPubSubTopicName = "langfuse-events-dev";
const prodLangfuseProjectId = "cm0ksv9qk000e14abow8adc4y";
const devLangfuseProjectId = "cm0kste92000814abo4uqx3rc";

const projectId = "liner-219011";

// Initialize PubSub publisher
const pubSubPublisherProd = new PubSubPublisher(prodPubSubTopicName, projectId);
const pubSubPublisherDev = new PubSubPublisher(devPubSubTopicName, projectId);

export const ingestionQueueProcessorBuilder = (
  enableRedirectToSecondaryQueue: boolean,
): Processor => {
  const projectIdsToRedirectToSecondaryQueue =
    env.LANGFUSE_SECONDARY_INGESTION_QUEUE_ENABLED_PROJECT_IDS?.split(",") ??
    [];

  return async (job: Job<TQueueJobTypes[QueueName.IngestionQueue]>) => {
    try {
      const span = getCurrentSpan();
      if (span) {
        span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
        span.setAttribute(
          "messaging.bullmq.job.input.projectId",
          job.data.payload.authCheck.scope.projectId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.eventBodyId",
          job.data.payload.data.eventBodyId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.type",
          job.data.payload.data.type,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.fileKey",
          job.data.payload.data.fileKey ?? "",
        );
      }

      // We write the new file into the ClickHouse event log to keep track for retention and deletions
      const clickhouseWriter = ClickhouseWriter.getInstance();

      if (
        env.LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG === "true" &&
        job.data.payload.data.fileKey &&
        job.data.payload.data.fileKey
      ) {
        const fileName = `${job.data.payload.data.fileKey}.json`;
        clickhouseWriter.addToQueue(TableName.BlobStorageFileLog, {
          id: randomUUID(),
          project_id: job.data.payload.authCheck.scope.projectId,
          entity_type: getClickhouseEntityType(job.data.payload.data.type),
          entity_id: job.data.payload.data.eventBodyId,
          event_id: job.data.payload.data.fileKey,
          bucket_name: env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
          bucket_path: `${env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX}${job.data.payload.authCheck.scope.projectId}/${getClickhouseEntityType(job.data.payload.data.type)}/${job.data.payload.data.eventBodyId}/${fileName}`,
          created_at: new Date().getTime(),
          updated_at: new Date().getTime(),
          event_ts: new Date().getTime(),
          is_deleted: 0,
        });
      }

      // If fileKey was processed within the last minutes, i.e. has a match in redis, we skip processing.
      if (
        env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE === "true" &&
        redis &&
        job.data.payload.data.fileKey
      ) {
        const key = `langfuse:ingestion:recently-processed:${job.data.payload.authCheck.scope.projectId}:${job.data.payload.data.type}:${job.data.payload.data.eventBodyId}:${job.data.payload.data.fileKey}`;
        const exists = await redis.exists(key);
        if (exists) {
          recordIncrement("langfuse.ingestion.recently_processed_cache", 1, {
            type: job.data.payload.data.type,
            skipped: "true",
          });
          logger.debug(
            `Skipping ingestion event ${job.data.payload.data.fileKey} for project ${job.data.payload.authCheck.scope.projectId}`,
          );
          return;
        } else {
          recordIncrement("langfuse.ingestion.recently_processed_cache", 1, {
            type: job.data.payload.data.type,
            skipped: "false",
          });
        }
      }

      if (
        enableRedirectToSecondaryQueue &&
        projectIdsToRedirectToSecondaryQueue.includes(
          job.data.payload.authCheck.scope.projectId,
        )
      ) {
        logger.debug(
          `Redirecting ingestion event to secondary queue for project ${job.data.payload.authCheck.scope.projectId}`,
        );
        const secondaryQueue = getQueue(QueueName.IngestionSecondaryQueue);
        if (secondaryQueue) {
          await secondaryQueue.add(QueueName.IngestionSecondaryQueue, job.data);
          // If we don't redirect, we continue with the ingestion. Otherwise, we finish here.
          return;
        }
      }

      const s3Client = getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      );

      logger.debug(
        `Processing ingestion event ${
          enableRedirectToSecondaryQueue ? "" : "secondary"
        }`,
        {
          projectId: job.data.payload.authCheck.scope.projectId,
          payload: job.data.payload.data,
        },
      );

      // Download all events from folder into a local array
      const clickhouseEntityType = getClickhouseEntityType(
        job.data.payload.data.type,
      );

      let eventFiles: { file: string; createdAt: Date }[] = [];
      const events: IngestionEventType[] = [];

      // Check if we should skip S3 list operation
      const shouldSkipS3List =
        // The producer sets skipS3List to true if it's an OTel observation
        job.data.payload.data.skipS3List && job.data.payload.data.fileKey;
      const s3Prefix = `${env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX}${job.data.payload.authCheck.scope.projectId}/${clickhouseEntityType}/${job.data.payload.data.eventBodyId}/`;

      let totalS3DownloadSizeBytes = 0;

      if (shouldSkipS3List) {
        // Direct file download - skip S3 list operation
        const filePath = `${s3Prefix}${job.data.payload.data.fileKey}.json`;
        eventFiles = [{ file: filePath, createdAt: new Date() }];

        const file = await s3Client.download(filePath);
        const fileSize = file.length;

        recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
          skippedS3List: "true",
        });
        totalS3DownloadSizeBytes += fileSize;

        const parsedFile = JSON.parse(file);
        events.push(...(Array.isArray(parsedFile) ? parsedFile : [parsedFile]));
      } else {
        eventFiles = await s3Client.listFiles(s3Prefix);

        // Process files in batches
        // If a user has 5k events, this will likely take 100 seconds.
        const downloadAndParseFile = async (fileRef: { file: string }) => {
          const file = await s3Client.download(fileRef.file);
          const fileSize = file.length;

          recordHistogram("langfuse.ingestion.s3_file_size_bytes", fileSize, {
            skippedS3List: "false",
          });
          totalS3DownloadSizeBytes += fileSize;

          const parsedFile = JSON.parse(file);
          return Array.isArray(parsedFile) ? parsedFile : [parsedFile];
        };

        const S3_CONCURRENT_READS = env.LANGFUSE_S3_CONCURRENT_READS;
        const batches = chunk(eventFiles, S3_CONCURRENT_READS);
        for (const batch of batches) {
          const batchEvents = await Promise.all(
            batch.map(downloadAndParseFile),
          );
          events.push(...batchEvents.flat());
        }
      }

      recordDistribution(
        "langfuse.ingestion.count_files_distribution",
        eventFiles.length,
        {
          kind: clickhouseEntityType,
        },
      );
      span?.setAttribute(
        "langfuse.ingestion.event.count_files",
        eventFiles.length,
      );
      span?.setAttribute("langfuse.ingestion.event.kind", clickhouseEntityType);
      span?.setAttribute(
        "langfuse.ingestion.s3_all_files_size_bytes",
        totalS3DownloadSizeBytes,
      );

      const firstS3WriteTime =
        eventFiles
          .map((fileRef) => fileRef.createdAt)
          .sort()
          .shift() ?? new Date();

      if (events.length === 0) {
        logger.warn(
          `No events found for project ${job.data.payload.authCheck.scope.projectId} and event ${job.data.payload.data.eventBodyId}`,
        );
        return;
      }

      // Set "seen" keys in Redis to avoid reprocessing for fast updates.
      // We use Promise.all internally instead of a redis.pipeline since autoPipelining should handle it correctly
      // while being redis cluster aware.
      if (env.LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE === "true" && redis) {
        try {
          await Promise.all(
            eventFiles
              .map((e) => e.file.split("/").pop() ?? "")
              .map((key) =>
                redis!.set(
                  `langfuse:ingestion:recently-processed:${job.data.payload.authCheck.scope.projectId}:${job.data.payload.data.type}:${job.data.payload.data.eventBodyId}:${key.replace(".json", "")}`,
                  "1",
                  "EX",
                  60 * 5, // 5 minutes
                ),
              ),
          );
        } catch (e) {
          logger.warn(
            `Failed to set recently-processed cache. Continuing processing.`,
            e,
          );
        }
      }

      // Perform merge of those events
      if (!redis) throw new Error("Redis not available");
      if (!prisma) throw new Error("Prisma not available");

      // Publish events to GCP PubSub
      try {
        try {
          const data = Buffer.from(JSON.stringify(events));
          if (
            job.data.payload.authCheck.scope.projectId == prodLangfuseProjectId
          ) {
            const messageId = await pubSubPublisherProd.publish(data);
            logger.debug(
              `Message ${messageId} published to topic ${prodPubSubTopicName}`,
            );
          } else if (
            job.data.payload.authCheck.scope.projectId == devLangfuseProjectId
          ) {
            const messageId = await pubSubPublisherDev.publish(data);
            logger.debug(
              `Message ${messageId} published to topic ${devPubSubTopicName}`,
            );
          } else {
            logger.warn(
              `Project ${job.data.payload.authCheck.scope.projectId} not in target list, skipping publish`,
            );
          }
        } catch (error) {
          logger.error("Error publishing messages to PubSub:", {
            error: error instanceof Error ? error.message : String(error),
            projectId: job.data.payload.authCheck.scope.projectId,
          });
          // Don't throw to allow rest of processing to continue
        }
      } catch (error) {
        logger.error("Error preparing events for PubSub publish:", error);
        // Don't throw error to allow rest of processing to continue
      }

      // Determine whether to forward to staging events table
      // Use explicit flag from job payload if provided, otherwise fall back to env flags
      const forwardToEventsTable =
        job.data.payload.data.forwardToEventsTable ??
        (env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE === "true" &&
          env.QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED === "true" &&
          env.LANGFUSE_EXPERIMENT_EARLY_EXIT_EVENT_BATCH_JOB !== "true");

      await new IngestionService(
        redis,
        prisma,
        clickhouseWriter,
        clickhouseClient(),
      ).mergeAndWrite(
        getClickhouseEntityType(events[0].type),
        job.data.payload.authCheck.scope.projectId,
        job.data.payload.data.eventBodyId,
        firstS3WriteTime,
        events,
        forwardToEventsTable,
      );
    } catch (e) {
      logger.error(
        `Failed job ingestion processing for ${job.data.payload.authCheck.scope.projectId}`,
        e,
      );
      traceException(e);
      throw e;
    }
  };
};
