package org.litesoft.aws.s3;

import org.litesoft.aws.clients.*;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;

public class AmazonS3Factory extends ClientFactory<AmazonS3> {
    public static final AmazonS3Factory INSTANCE = new AmazonS3Factory();

    @Override
    protected AmazonS3 create( AWSCredentials pCredentials ) {
        return new AmazonS3Client( pCredentials );
    }
}
