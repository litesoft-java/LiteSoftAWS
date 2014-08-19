package org.litesoft.aws.s3;

import org.litesoft.aws.credentials.*;
import org.litesoft.commonfoundation.base.*;
import org.litesoft.commonfoundation.console.*;
import org.litesoft.commonfoundation.indent.*;
import org.litesoft.server.file.*;

import java.io.*;

public class BucketCredentials {
    public static final String CREDENTIALS_PREFIX = "AwsCredentials";
    public static final String CREDENTIALS_SUFFIX = ".properties";

    private final String mBucketName;
    private IndentableWriter mWriter;

    private BucketCredentials( String pBucketName ) {
        mBucketName = pBucketName;
    }

    private IndentableWriter getWriter() {
        return (mWriter != null) ? mWriter : new ConsoleIndentableWriter( ConsoleSOUT.INSTANCE );
    }

    public static BucketCredentials with( String pBucketName ) {
        return new BucketCredentials( Confirm.significant( "BucketName", pBucketName ) );
    }

    public BucketCredentials and( IndentableWriter pWriter ) {
        mWriter = ConstrainTo.notNull( pWriter, IndentableWriter.NULL );
        return this;
    }

    public CachedAWSCredentials get( String... pFilePrefixes ) {
        CachedAWSCredentials zCredentials = load( mBucketName, pFilePrefixes );
        if ( zCredentials.hasCredentials() ) {
            return zCredentials;
        }
        if ( zCredentials.fileExists() ) {
            ErrorBoxingIndentableWriter zWriter = new ErrorBoxingIndentableWriter( getWriter() );
            zCredentials.report( zWriter );
            zWriter.printLn( zCredentials.getError() );
            zWriter.close();
        }
        return load( null, pFilePrefixes );
    }

    private CachedAWSCredentials load( String pSpecificType, String[] pFilePrefixes ) {
        String zFileName = CREDENTIALS_PREFIX +
                           ((null != (pSpecificType = ConstrainTo.significantOrNull( pSpecificType ))) ? "-" + pSpecificType : "") +
                           CREDENTIALS_SUFFIX;
        File zFile = DirectoryUtils.findAncestralFile( new File( FileUtils.currentWorkingDirectory() ), zFileName, pFilePrefixes );
        CachedAWSCredentials zCredentials = new CachedAWSCredentials( (zFile == null) ? zFileName : zFile.getPath() );
        if ( zCredentials.hasCredentials() ) {
            zCredentials.report( getWriter() );
        }
        return zCredentials;
    }
}
