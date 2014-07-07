package org.litesoft.aws.s3;

import org.litesoft.aws.credentials.*;
import org.litesoft.commonfoundation.base.*;
import org.litesoft.commonfoundation.exceptions.*;
import org.litesoft.commonfoundation.typeutils.*;
import org.litesoft.server.util.*;

import com.amazonaws.services.s3.internal.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.*;

import java.io.*;
import java.util.*;

public class S3Persister extends S3ClientSupport {
    private final CannedAccessControlList mCannedAclForAllS3Objects;

    public S3Persister( CachedAWSCredentials pCredentials, Bucket pBucket, CannedAccessControlList pCannedAclForAllS3Objects )
            throws IOException {
        super( pCredentials, pBucket );
        mCannedAclForAllS3Objects = ConstrainTo.notNull( pCannedAclForAllS3Objects, CannedAccessControlList.PublicRead );
    }

    public S3Persister( CachedAWSCredentials pCredentials, Bucket pBucket )
            throws IOException {
        this( pCredentials, pBucket, null );
    }

    @Override
    public String[] getTextFile( String pPath )
            throws FileSystemException {
        InputStream zInputStream = getFile( pPath );
        boolean zClosed = false;
        try {
            String[] zLines = IOUtils.loadTextFile( IOUtils.createReader( zInputStream ) );
            zClosed = true;
            return zLines;
        }
        catch ( Exception e ) {
            throw convert( e, pPath );
        }
        finally {
            if ( !zClosed ) {
                IOUtils.drain( zInputStream );
                Closeables.dispose( zInputStream );
            }
        }
    }

    @Override
    public void putTextFile( String pPath, String[] pLines )
            throws FileSystemException {
        try {
            putFile( pPath, new StringInputStream( Strings.mergeLines( pLines ) ) );
        }
        catch ( UnsupportedEncodingException e ) {
            throw new FileSystemException( e );
        }
    }

    @Override
    public InputStream getFile( String pPath )
            throws FileSystemException {
        try {
            S3Object zObject = mClient.getObject( getBucketName(), pPath );
            return zObject.getObjectContent();
        }
        catch ( Exception e ) {
            throw convert( e, pPath );
        }
    }

    @Override
    public void putFile( String pPath, InputStream pFileContents )
            throws FileSystemException {
        ObjectMetadata zMetadata = new ObjectMetadata();
        if ( shouldCache( pPath ) ) {
            cacheForever( zMetadata );
        } else {
            noCache( zMetadata );
        }
        zMetadata.setContentType( Mimetypes.getInstance().getMimetype( extractFileName( pPath ) ) );
        boolean zClosed = false;
        try {
            mClient.putObject( addACL( new PutObjectRequest( getBucketName(), pPath, pFileContents, zMetadata ) ) );
            zClosed = true;
        }
        catch ( Exception e ) {
            throw convert( e, pPath );
        }
        finally {
            if ( !zClosed ) {
                IOUtils.drain( pFileContents );
                Closeables.dispose( pFileContents );
            }
        }
    }

    protected PutObjectRequest addACL( PutObjectRequest pRequest ) {
        return pRequest.withCannedAcl( getCannedAclForAllS3Objects() );
    }

    @Override
    public String[] getDirectories( String pDirectoryNamePrefix )
            throws FileSystemException {
        return getKeyList( pDirectoryNamePrefix, DIRECTORY_S3LISTFILTER );
    }

    @Override
    public String[] getFiles( String pFilesSubDirectory, String pFileNamePrefix, final String pFileExtension )
            throws FileSystemException {
        String zKeyPrefix = pFileNamePrefix;
        if ( (pFilesSubDirectory = ConstrainTo.significantOrNull( pFilesSubDirectory, "" )).length() != 0 ) {
            zKeyPrefix = (pFilesSubDirectory += "/") + pFileNamePrefix;
        }
        final int zSkipLength = pFilesSubDirectory.length();
        return getKeyList( zKeyPrefix, new S3ListFilter() {
            @Override
            public boolean filteredAdd( String pPath, Collection<String> pCollector ) {
                int zAt = (pPath = pPath.substring( zSkipLength )).indexOf( '/' );
                if ( (zAt == -1) && pPath.endsWith( pFileExtension ) ) {
                    pCollector.add( pPath );
                }
                return true;
            }
        } );
    }

    @Override
    public String[] getAllFilesUnder( String pFilesSubDirectory )
            throws FileSystemException {
        return getKeyList( Confirm.significant( "FilesSubDirectory", pFilesSubDirectory ), new S3ListFilter() {
            @Override
            public boolean filteredAdd( String pPath, Collection<String> pCollector ) {
                pCollector.add( pPath );
                return true;
            }
        } );
    }

    @Override
    public void copyFile( String pSourcePath, String pDestinationPath )
            throws FileSystemException {
        copyFileFromUsingS3sObjectCopy( getBucketName(), pSourcePath, pDestinationPath );
    }

    @Override
    public void copyFile( Persister pSourcePersister, String pSourcePath, String pDestinationPath )
            throws FileSystemException {
        S3Persister them = checkS3CopyFriendly( pSourcePersister );
        if ( them != null ) {
            copyFileFromUsingS3sObjectCopy( them.getBucketName(), pSourcePath, pDestinationPath );
            return;
        }
        super.copyFile( pSourcePersister, pSourcePath, pDestinationPath ); // Read and then Write!
    }

    @Override
    public void deleteDirectory( String pPath )
            throws FileSystemException {
        String zKeyPrefix = Strings.replace( pPath + "/", "//", "/" );
        for ( Set<String> zKeysToDelete = getKeySet( zKeyPrefix, DELETE_S3LISTFILTER ); !zKeysToDelete.isEmpty(); ) {
            try {
                mClient.deleteObjects( new DeleteObjectsRequest( getBucketName() ).withKeys( zKeysToDelete.toArray( new String[zKeysToDelete.size()] ) ) );
            }
            catch ( MultiObjectDeleteException e ) {
                e.printStackTrace(); // Fall thru...
            }
            catch ( Exception e ) {
                throw convert( e, zKeyPrefix );
            }
            Set<String> zKeysToDelete2 = getKeySet( zKeyPrefix, DELETE_S3LISTFILTER );
            if ( zKeysToDelete.equals( zKeysToDelete2 ) ) {
                throw new FileSystemException( "Unable to delete: " + zKeysToDelete );
            }
            zKeysToDelete = zKeysToDelete2;
        }
    }

    @Override
    public void deleteFile( String pPath )
            throws FileSystemException {
        try {
            mClient.deleteObject( getBucketName(), pPath );
        }
        catch ( Exception e ) {
            throw convert( e, pPath );
        }
    }

    @Override
    public boolean isReadable( String pPath )
            throws FileSystemException {
        AccessControlList zACL;
        try {
            zACL = mClient.getObjectAcl( getBucketName(), pPath );
        }
        catch ( Exception e ) {
            throw convert( e, pPath );
        }
        return zACL.getGrants().contains( new Grant( GroupGrantee.AllUsers, Permission.Read ) );
    }

    private void copyFileFromUsingS3sObjectCopy( String pSourceS3BucketName, String pSourcePath, String pDestinationPath )
            throws FileSystemException {
        try {
            mClient.copyObject( addACL( new CopyObjectRequest( pSourceS3BucketName, pSourcePath, getBucketName(), pDestinationPath ) ) );
        }
        catch ( Exception e ) {
            throw convert( e, pDestinationPath + " <- " + pSourceS3BucketName + ":" + pSourcePath );
        }
    }

    protected CopyObjectRequest addACL( CopyObjectRequest pRequest ) {
        return pRequest.withCannedAccessControlList( getCannedAclForAllS3Objects() );
    }

    protected CannedAccessControlList getCannedAclForAllS3Objects() {
        return mCannedAclForAllS3Objects;
    }

    private String extractFileName( String pPath ) {
        int index = pPath.lastIndexOf( '/' );
        return (index == -1) ? pPath : pPath.substring( index + 1 );
    }

    private static S3ListFilter DIRECTORY_S3LISTFILTER = new S3ListFilter() {
        @Override
        public boolean filteredAdd( String pPath, Collection<String> pCollector ) {
            int zAt = pPath.indexOf( '/' );
            if ( zAt != -1 ) {
                pCollector.add( pPath.substring( 0, zAt ) );
            }
            return true;
        }
    };

    private static S3ListFilter DELETE_S3LISTFILTER = new S3ListFilter() {
        @Override
        public boolean filteredAdd( String pPath, Collection<String> pCollector ) {
            pCollector.add( pPath );
            return pCollector.size() < 5;
        }
    };

    private final Map<Persister, Boolean> mCachedCheckS3CopyFriendly = Maps.newIdentityHashMap();

    private S3Persister checkS3CopyFriendly( Persister pPersister ) {
        Boolean zFriendly;
        synchronized ( mCachedCheckS3CopyFriendly ) {
            zFriendly = mCachedCheckS3CopyFriendly.get( pPersister );
            if ( zFriendly == null ) {
                mCachedCheckS3CopyFriendly.put( pPersister, zFriendly = isS3CopyFriendly( pPersister ) );
            }
        }
        return zFriendly ? (S3Persister) pPersister : null;
    }

    private synchronized boolean isS3CopyFriendly( Persister pPersister ) {
        return (pPersister instanceof S3Persister) && super.isS3CopyFriendly( (S3ClientSupport) pPersister );
    }
}
