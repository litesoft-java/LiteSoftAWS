package org.litesoft.aws.s3;

import org.litesoft.aws.credentials.*;
import org.litesoft.commonfoundation.base.*;
import org.litesoft.commonfoundation.exceptions.*;
import org.litesoft.commonfoundation.typeutils.*;
import org.litesoft.server.util.*;

import com.amazonaws.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.text.*;
import java.util.*;

public abstract class S3ClientSupport extends Persister {
    private final CachedAWSCredentials mCredentials;
    private final Bucket mBucket;
    protected final AmazonS3 mClient;
    protected final String m2Jan1970, mOneYearFromNow;

    protected S3ClientSupport( CachedAWSCredentials pCredentials, Bucket pBucket )
            throws IOException {
        mCredentials = Confirm.isNotNull( "Credentials", pCredentials );
        mBucket = Confirm.isNotNull( "Bucket", pBucket );
        System.setProperty( "org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog" );
        mClient = AmazonS3Factory.INSTANCE.supplierWith( pCredentials ).get();
        mClient.setEndpoint( pBucket.getS3Endpoint() ); // Other Regions - See: http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
        // RFC 1123: "Thu, 01 Dec 1994 16:00:00 GMT"
        String fmt = "EEE, dd MMM yyyy HH:mm:ss zzz";
        SimpleDateFormat zFormatter = new SimpleDateFormat( fmt, Locale.US );
        zFormatter.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
        m2Jan1970 = zFormatter.format( new Date( 86400000 ) ); // January 2, 1970
        mOneYearFromNow = zFormatter.format( new Date( System.currentTimeMillis() + 31536000000l ) );
    }

    @Override
    public String toString() {
        return mBucket.toString();
    }

    protected String getBucketName() {
        return mBucket.getName();
    }

    protected boolean shouldCache( String pRelativeFilePath ) {
        return !pRelativeFilePath.toLowerCase().endsWith( ".html" );
    }

    protected void cacheForever( ObjectMetadata pMetadata ) {
        // the w3c spec requires a maximum age of 1 year
        setCacheHeaders( pMetadata, "public max-age=31536000", mOneYearFromNow );
    }

    protected void noCache( ObjectMetadata pMetadata ) {
        setCacheHeaders( pMetadata, "no-cache no-store must-revalidate", m2Jan1970 );
    }

    protected void setCacheHeaders( ObjectMetadata pMetadata, String pCacheControl, String pExpires ) {
        pMetadata.setCacheControl( pCacheControl );
        pMetadata.setHeader( "Expires", pExpires );
    }

    protected FileSystemException convert( Exception e, String pPostBucketText ) {
        String zMessage = getBucketName() + ":" + pPostBucketText;
        if ( e instanceof AmazonClientException ) {
            String s = Throwables.printStackTraceToString( e );
            if ( s.contains( "AccessDenied" ) ) {
                return new AccessDeniedFileSystemException( zMessage, e );
            }
        }
        return new FileSystemException( zMessage, e );
    }

    protected interface S3ListFilter {
        /**
         * @return True to continue adding.
         */
        boolean filteredAdd( String pPath, Collection<String> pCollector );
    }

    protected Set<String> getKeySet( String pKeyPrefix, S3ListFilter pFilter ) {
        Set<String> zFound = new HashSet<String>();
        try {
            for ( S3ListObjectsIterator zIt = new S3ListObjectsIterator( mClient, getBucketName(), pKeyPrefix ); zIt.hasNext(); ) {
                pFilter.filteredAdd( zIt.next().getKey(), zFound );
            }
        }
        catch ( Exception e ) {
            throw convert( e, pKeyPrefix );
        }
        return zFound;
    }

    protected String[] getKeyList( String pKeyPrefix, S3ListFilter pFilter ) {
        Set<String> zFound = getKeySet( pKeyPrefix, pFilter );
        return zFound.toArray( new String[zFound.size()] );
    }

    protected boolean isS3CopyFriendly( S3ClientSupport them ) {
        return (this.mCredentials != null) && this.mCredentials.isS3CopyFriendly( them.mCredentials );
    }

    private static class S3ListObjectsIterator implements Iterator<S3ObjectSummary> {
        private final AmazonS3 mClient;
        private ObjectListing mObjectListing;
        private Iterator<S3ObjectSummary> mCurIterator;

        public S3ListObjectsIterator( AmazonS3 pClient, String pBucketName, String pPrefix ) {
            mClient = pClient;
            handleBlock( mClient.listObjects( pBucketName, pPrefix ) );
        }

        private Iterator<S3ObjectSummary> handleBlock( ObjectListing pObjectListing ) {
            mObjectListing = pObjectListing;
            return mCurIterator = mObjectListing.getObjectSummaries().iterator();
        }

        @SuppressWarnings("SimplifiableIfStatement")
        @Override
        public boolean hasNext() {
            if ( mCurIterator.hasNext() ) {
                return true;
            }
            if ( mObjectListing.isTruncated() ) {
                return handleBlock( mClient.listNextBatchOfObjects( mObjectListing ) ).hasNext();
            }
            return false;
        }

        @Override
        public S3ObjectSummary next() {
            if ( hasNext() ) {
                return mCurIterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
