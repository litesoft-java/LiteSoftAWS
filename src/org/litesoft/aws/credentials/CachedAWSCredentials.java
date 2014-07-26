package org.litesoft.aws.credentials;

import org.litesoft.commonfoundation.base.*;
import org.litesoft.commonfoundation.console.*;
import org.litesoft.commonfoundation.exceptions.*;
import org.litesoft.commonfoundation.indent.*;

import com.amazonaws.auth.*;
import java8.util.function.*;

import java.io.*;

public class CachedAWSCredentials implements Supplier<AWSCredentials> {
    private final boolean mFileFound;
    private final String mCredentialsPropertiesFileName;
    private final AWSCredentials mAwsCredentials;
    private final IOException mError;
    private boolean mReported;

    public CachedAWSCredentials( String pCredentialsPropertiesFileName ) {
        File zFile = new File( Confirm.significant( "CredentialsPropertiesFileName", pCredentialsPropertiesFileName ) );
        mFileFound = zFile.isFile();
        AWSCredentials zAwsCredentials = null;
        IOException zError = null;
        try {
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

    public boolean fileExists() {
        return mFileFound;
    }

    public String getCredentialsPropertiesFileName() {
        return mCredentialsPropertiesFileName;
    }

    public boolean hasCredentials() {
        return (mAwsCredentials != null);
    }

    public void report() {
        report( new ConsoleIndentableWriter( "", ConsoleSOUT.INSTANCE ) );
    }

    public void report( IndentableWriter pWriter ) {
        if ( (pWriter != null) && !mReported ) {
            mReported = true;
            pWriter.printLn( "Using: " + mCredentialsPropertiesFileName );
        }
    }

    @Override
    public AWSCredentials get() {
        if ( hasCredentials() ) {
            report();
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
