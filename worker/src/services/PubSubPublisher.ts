import { GoogleAuth } from 'google-auth-library';
import { logger } from "@langfuse/shared/src/server";

interface PubSubPublishResponse {
  messageIds: string[];
}

interface RetryConfig {
  maxRetries: number;
  baseDelay: number;
  maxDelay: number;
  backoffMultiplier: number;
}

export class PubSubPublisher {
  private readonly topicName: string;
  private readonly projectId: string;
  private readonly auth: GoogleAuth;
  private readonly key: any;
  private readonly retryConfig: RetryConfig;
  private cachedToken: { token: string; expiry: number } | null = null;

  constructor(
    topicName: string, 
    projectId: string, 
    retryConfig?: Partial<RetryConfig>
  ) {
    this.topicName = topicName;
    this.projectId = projectId;
    this.key = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS || '{}');
    this.auth = new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/pubsub'],
      projectId: projectId,
      credentials: this.key,
    });
    
    // Default retry configuration
    this.retryConfig = {
      maxRetries: retryConfig?.maxRetries ?? 10,
      baseDelay: retryConfig?.baseDelay ?? 1000, // 1 second
      maxDelay: retryConfig?.maxDelay ?? 30000, // 30 seconds
      backoffMultiplier: retryConfig?.backoffMultiplier ?? 2,
    };
  }

  private async getAuthToken(): Promise<string> {
    const now = Date.now();
    
    // If we have a cached token that's still valid (with 5 minute buffer), use it
    if (this.cachedToken && this.cachedToken.expiry > now + 5 * 60 * 1000) {
      return this.cachedToken.token;
    }

    // Get new token
    const client = await this.auth.getClient();
    const { token } = await client.getAccessToken();
    
    
    if (!token) {
      throw new Error('Failed to get access token');
    }

    // Get token expiry from client
    const expiryDate = client.credentials.expiry_date;
    
    // Cache the new token
    this.cachedToken = {
      token,
      expiry: expiryDate || (now + 3600 * 1000), // Default to 1 hour if expiry not provided
    };
    
    return token;
  }

  private shouldRetry(error: any): boolean {
    // Don't retry on authentication errors or invalid requests
    if (error.message?.includes('502')) {
      return false;
    }
    
    // Retry on network errors, timeouts, and server errors
    return true;
  }

  private async delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private calculateDelay(attempt: number): number {
    const delay = this.retryConfig.baseDelay * Math.pow(this.retryConfig.backoffMultiplier, attempt - 1);
    return Math.min(delay, this.retryConfig.maxDelay);
  }

  async publish(data: Buffer): Promise<string> {
    let lastError: Error | null = null;
    
    for (let attempt = 1; attempt <= this.retryConfig.maxRetries + 1; attempt++) {
      try {
        const url = `https://pubsub.googleapis.com/v1/projects/${this.projectId}/topics/${this.topicName}:publish`;
        
        // Get authentication token (will use cached token if available)
        const token = await this.getAuthToken();
        
        const response = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
          },
          body: JSON.stringify({
            messages: [{
              data: data.toString('base64')
            }]
          })
        });

        if (!response.ok) {
          const errorText = await response.text();
          const error = new Error(`Failed to publish to PubSub: ${response.status} ${response.statusText} - ${errorText}`);
          
          // Check if we should retry this error
          if (attempt <= this.retryConfig.maxRetries && this.shouldRetry(error)) {
            lastError = error;
            const delayMs = this.calculateDelay(attempt);
            
            logger.warn(`PubSub publish attempt ${attempt} failed, retrying in ${delayMs}ms:`, {
              error: error.message,
              status: response.status,
              projectId: this.projectId,
              topicName: this.topicName
            });
            
            await this.delay(delayMs);
            continue;
          }
          
          throw error;
        }

        const result = await response.json() as PubSubPublishResponse;
        const messageId = result.messageIds?.[0];
        
        if (attempt > 1) {
          logger.info(`PubSub publish succeeded on attempt ${attempt}:`, { messageId });
        } else {
          logger.debug('Successfully published to PubSub:', { messageId });
        }
        
        return messageId;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        
        // If this is the last attempt or we shouldn't retry, throw the error
        if (attempt > this.retryConfig.maxRetries || !this.shouldRetry(lastError)) {
          logger.error('Error publishing to PubSub after all retry attempts:', {
            error: lastError.message,
            stack: lastError.stack,
            projectId: this.projectId,
            topicName: this.topicName,
            attempts: attempt
          });
          throw lastError;
        }
        
        // Calculate delay for next retry
        const delayMs = this.calculateDelay(attempt);
        
        logger.warn(`PubSub publish attempt ${attempt} failed, retrying in ${delayMs}ms:`, {
          error: lastError.message,
          projectId: this.projectId,
          topicName: this.topicName
        });
        
        await this.delay(delayMs);
      }
    }
    
    // This should never be reached, but just in case
    throw lastError || new Error('Unknown error occurred during PubSub publish');
  }
} 