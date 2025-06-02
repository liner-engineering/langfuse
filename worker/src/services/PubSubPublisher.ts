import { GoogleAuth } from 'google-auth-library';
import { logger } from "@langfuse/shared/src/server";

interface PubSubPublishResponse {
  messageIds: string[];
}

export class PubSubPublisher {
  private readonly topicName: string;
  private readonly projectId: string;
  private readonly auth: GoogleAuth;
  private readonly key: any;
  private cachedToken: { token: string; expiry: number } | null = null;

  constructor(topicName: string, projectId: string) {
    this.topicName = topicName;
    this.projectId = projectId;
    this.key = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS || '{}');
    this.auth = new GoogleAuth({
      scopes: ['https://www.googleapis.com/auth/pubsub'],
      projectId: projectId,
      credentials: this.key,
    });
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

  async publish(data: Buffer): Promise<string> {
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
        throw new Error(`Failed to publish to PubSub: ${response.status} ${response.statusText} - ${errorText}`);
      }

      const result = await response.json() as PubSubPublishResponse;
      const messageId = result.messageIds?.[0];
      logger.debug('Successfully published to PubSub:', { messageId });
      return messageId;
    } catch (error) {
      logger.error('Error publishing to PubSub:', {
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
        projectId: this.projectId,
        topicName: this.topicName
      });
      throw error;
    }
  }
} 