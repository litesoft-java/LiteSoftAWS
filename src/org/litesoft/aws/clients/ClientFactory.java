package org.litesoft.aws.clients;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import java8.util.function.*;

public abstract class ClientFactory<Client> {
    public final Supplier<Client> supplierWith( final Supplier<AWSCredentials> pCredentialsSupplier ) {
        return new Supplier<Client>() {
            @Override
            public Client get() {
                try {
                    return create( pCredentialsSupplier.get() );
                }
                catch ( AmazonServiceException ase ) {
                    /*
                    * AmazonServiceExceptions represent an error response from an AWS
                    * services, i.e. your request made it to AWS, but the AWS service
                    * either found it invalid or encountered an error trying to execute
                    * it.
                    */
                    System.out.println( "Error Message:    " + ase.getMessage() );
                    System.out.println( "HTTP Status Code: " + ase.getStatusCode() );
                    System.out.println( "AWS Error Code:   " + ase.getErrorCode() );
                    System.out.println( "Error Type:       " + ase.getErrorType() );
                    System.out.println( "Request ID:       " + ase.getRequestId() );
                    /*
                    * AmazonClientExceptions represent an error that occurred inside
                    * the client on the local host, either while trying to send the
                    * request to AWS or interpret the response. For example, if no
                    * network connection is available, the client won't be able to
                    * connect to AWS to execute a request and will throw an
                    * AmazonClientException.
                    */
                    throw new RuntimeException( ase );
                }
            }
        };
    }

    abstract protected Client create( AWSCredentials pCredentials );
}
