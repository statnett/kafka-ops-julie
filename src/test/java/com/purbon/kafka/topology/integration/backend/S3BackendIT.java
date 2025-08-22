package com.purbon.kafka.topology.integration.backend;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.Configuration;
import com.purbon.kafka.topology.backend.BackendState;
import com.purbon.kafka.topology.backend.S3Backend;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.robothy.s3.rest.LocalS3;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class S3BackendIT {

  private LocalS3 api;
  private Map<String, String> cliOps;

  private static final String TEST_BUCKET = "testbucket";
  private static final String TEST_ENDPOINT = "http://127.0.0.1:8001";

  @Before
  public void before() {
    cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    long time = System.currentTimeMillis();
    String tmpDir = System.getProperty("java.io.tmpdir");
    String s3Path = Paths.get(tmpDir, "s3-" + time).toString();
    api = LocalS3.builder().port(8001).dataPath(s3Path).build();
    api.start();

    S3Client client =
        S3Client.builder()
            .region(Region.US_WEST_2)
            .endpointOverride(URI.create(TEST_ENDPOINT))
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .build();
    client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
  }

  @After
  public void after() {
    api.shutdown();
  }

  @Test
  public void testContentCreation() throws IOException {

    S3Backend backend = new S3Backend();

    Properties props = new Properties();
    props.put(JULIE_S3_REGION, "us-west-2");
    props.put(JULIE_S3_BUCKET, TEST_BUCKET);
    props.put(JULIE_S3_ENDPOINT, TEST_ENDPOINT);

    Configuration config = new Configuration(cliOps, props);
    backend.configure(config, true);

    backend.load();

    TopologyAclBinding binding =
        TopologyAclBinding.build(
            ResourceType.TOPIC.name(),
            "foo",
            "*",
            "Write",
            "User:foo",
            "LITERAL",
            AclPermissionType.ALLOW.name());

    BackendState state = new BackendState();
    state.addBindings(Collections.singleton(binding));
    backend.save(state);
    backend.close();

    S3Backend newBackend = new S3Backend();
    newBackend.configure(config, true);

    BackendState newState = newBackend.load();
    assertThat(newState.size()).isEqualTo(1);
    assertThat(newState.getBindings()).contains(binding);
  }
}
