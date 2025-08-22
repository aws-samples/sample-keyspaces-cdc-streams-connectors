package software.amazon.ssa.streams.helpers;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSHelpers {

  private static final Logger logger = LoggerFactory.getLogger(AWSHelpers.class);
    
    public static String createWorkerIdFromSTS() {
    // 4) Generic AWS: STS caller identity ARN
        try (StsClient sts = StsClient.builder()
            .region(Region.AWS_GLOBAL)
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .build()) {
        GetCallerIdentityResponse id = sts.getCallerIdentity();
        
        String arn = id.arn(); // arn:aws:sts::<acct>:assumed-role/<role-name>/<session-name>
        // Try to extract the role session-name for a compact ID
        String sess = extractSessionName(arn);
        return sess != null ? "sts:" + shortId(sess) : "sts:" + shortId(arn);
        } catch (SdkClientException e) {
            logger.error("Error getting caller identity", e);
        // ignore and fall through
        }

        String guid = UUID.randomUUID().toString();
        logger.error("Error getting caller identity, using random GUID: {}", guid);
        return guid;
    }
    private static String extractSessionName(String arn) {
        // arn:...:assumed-role/<role-name>/<session-name>
        Matcher m = Pattern.compile("assumed-role/[^/]+/([^/]+)$").matcher(arn);
        return m.find() ? m.group(1) : arn;
        // On EC2 this is often i-xxxxxxxx; on ECS contains the task id.
      }
      private static String shortId(String s) {
        // simple shortener: last 256 characters
        if (s == null) return "";
        return s.length() > 256 ? s.substring(s.length() - 256) : s;
      }
}
