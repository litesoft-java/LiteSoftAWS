package org.litesoft.aws.s3;

import org.litesoft.aws.credentials.*;
import org.litesoft.commonfoundation.base.*;
import org.litesoft.server.file.*;

import java.io.*;

public class BucketCredentials {

    public static final String CREDENTIALS_PREFIX = "AwsCredentials";
    public static final String CREDENTIALS_SUFFIX = ".properties";

    public static CachedAWSCredentials get( String pBucketName ) {
        CachedAWSCredentials zCredentials = load( Confirm.significant( "BucketName", pBucketName ) );
        if ( zCredentials.hasCredentials() ) {
            return zCredentials;
        }
        if ( zCredentials.fileExists() ) {
            zCredentials.report( System.err );
            System.err.println( zCredentials.getError() );
        }
        return load( null );
    }

    private static CachedAWSCredentials load( String pSpecificType ) {
        String zFileName = CREDENTIALS_PREFIX +
                           ((null != (pSpecificType = ConstrainTo.significantOrNull( pSpecificType ))) ? "-" + pSpecificType : "") +
                           CREDENTIALS_SUFFIX;
        File zFile = DirectoryUtils.findAncestralFile( new File( FileUtils.currentWorkingDirectory() ), zFileName );
        return new CachedAWSCredentials( (zFile == null) ? zFileName : zFile.getPath() );
    }
}
