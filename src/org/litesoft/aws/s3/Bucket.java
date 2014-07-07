package org.litesoft.aws.s3;

import org.litesoft.commonfoundation.base.*;
import org.litesoft.commonfoundation.typeutils.*;

public class Bucket {
    protected final String mS3Endpoint;
    protected final String mName;
    protected final String mCloudFrontURL;

    public Bucket( String pS3Endpoint, String pName, String pCloudFrontURL ) {
        mS3Endpoint = Confirm.isNotNull( "S3Endpoint", pS3Endpoint );
        mName = Confirm.significant( "Name", pName );
        mCloudFrontURL = ConstrainTo.significantOrNull( pCloudFrontURL );
    }

    public Bucket( String pS3Endpoint, String pName ) {
        this( pS3Endpoint, pName, null );
    }

    public String getS3Endpoint() {
        return mS3Endpoint;
    }

    public String getName() {
        return mName;
    }

    public String getCloudFrontURL() {
        return mCloudFrontURL;
    }

    @Override
    public String toString() {
        MsgBuilder zBuilder = new MsgBuilder().addText( mName );
        addS3EndpointStart( zBuilder );
        addCloudFrontURL( zBuilder );
        addS3EndpointEnd( zBuilder );
        return zBuilder.toString();
    }

    protected void addS3EndpointStart( MsgBuilder pBuilder ) {
        pBuilder.addOptionalText( "(", mS3Endpoint );
    }

    protected void addS3EndpointEnd( MsgBuilder pBuilder ) {
        pBuilder.addText( ")" );
    }

    protected void addCloudFrontURL( MsgBuilder pBuilder ) {
        pBuilder.addOptionalText( "(", mS3Endpoint );
    }
}
