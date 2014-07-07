package org.litesoft.aws.credentials;

import org.litesoft.commonfoundation.exceptions.*;

import com.amazonaws.auth.*;
import java8.util.function.*;

import java.io.*;

public class CachedAWSCredentials implements Supplier<AWSCredentials> {
    private final String mCredentialsPropertiesFileName;
    private final AWSCredentials mAwsCredentials;
    private final IOException mError;
    private boolean mReported;

    public CachedAWSCredentials( String pCredentialsPropertiesFileName ) {
        AWSCredentials zAwsCredentials = null;
        IOException zError = null;
        try {
            File zFile = new File( pCredentialsPropertiesFileName );
            pCredentialsPropertiesFileName = zFile.getCanonicalPath();
            zAwsCredentials = new PropertiesCredentials( new FileInputStream( pCredentialsPropertiesFileName ) );
        }
        catch ( IOException e ) {
            zError = e;
        }
        mCredentialsPropertiesFileName = pCredentialsPropertiesFileName;
        mAwsCredentials = zAwsCredentials;
        mError = zError;
    }

    public boolean hasCredentials() {
        return (mAwsCredentials != null);
    }

    @Override
    public AWSCredentials get() {
        if ( hasCredentials() ) {
            if ( !mReported ) {
                mReported = true;
                System.out.println( "Using: " + mCredentialsPropertiesFileName );
            }
            return mAwsCredentials;
        }
        throw new FileSystemException( mError );
    }

    public IOException getError() {
        return mError;
    }

    public boolean isS3CopyFriendly( CachedAWSCredentials them ) {
        return (them != null) && this.hasCredentials() && them.hasCredentials() && areEqual( this.mAwsCredentials, them.mAwsCredentials );
    }

    private static boolean areEqual( AWSCredentials pCredentials1, AWSCredentials pCredentials2 ) {
        return pCredentials1.getAWSAccessKeyId().equals( pCredentials2.getAWSAccessKeyId() )
               && pCredentials1.getAWSSecretKey().equals( pCredentials2.getAWSSecretKey() );
    }
}
