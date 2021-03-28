package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

// Basic client with tls
// KubeMQ server private key and cer is is required to configure on server
// How to generate certs and key
// get mkcert from: https://github.com/FiloSottile/mkcert
// create cert and keys: mkcert localhost
// install key and cert in kubemq
// WARNING !! - this process is not suitable for production

const cert = `-----BEGIN CERTIFICATE-----
MIIEZTCCAs2gAwIBAgIQNakMiOugpZk/xMBRgiaAyTANBgkqhkiG9w0BAQsFADCB
kTEeMBwGA1UEChMVbWtjZXJ0IGRldmVsb3BtZW50IENBMTMwMQYDVQQLDCpERVNL
VE9QLUxOQjdQMjBcbGlvci5uYWJhdEBERVNLVE9QLUxOQjdQMjAxOjA4BgNVBAMM
MW1rY2VydCBERVNLVE9QLUxOQjdQMjBcbGlvci5uYWJhdEBERVNLVE9QLUxOQjdQ
MjAwHhcNMjEwMzI4MDkxMDQ4WhcNMjMwNjI4MDkxMDQ4WjBrMScwJQYDVQQKEx5t
a2NlcnQgZGV2ZWxvcG1lbnQgY2VydGlmaWNhdGUxQDA+BgNVBAsMN0RFU0tUT1At
TE5CN1AyMFxsaW9yLm5hYmF0QERFU0tUT1AtTE5CN1AyMCAoTGlvciBOYWJhdCkw
ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC75DABpmw0761x1sbWh8Bk
Fz2T8Pq/I38TFtbP+Jc3bUCSK5O6HGOBsua47yVyG8Wsy1DWImoVKFvHkCvqD2fb
OR0OwZ285aAJhElF1vNiSxAXtdeNnmc8Y3O0DAaOkcs+YGQ/cGb2Tu62xZlLtBUO
3NabdYDxHiqN6wzAmXEcJ3flwXm4ysLhUaYdfEmyABh6gmShifEbT+xXtj4tfTDA
AKtLGZRDlSTXCXRrRP6GSSjgapUCOeT+WFc1SkRSVN4HTARVfJqLgX2TYDeO4ZDh
1uk5ZN3oUY94jt/Y/Wd0Aws7tP1NsXpTt+YUqGQtnALwB3nqsQtaillhiM9Sa5Ch
AgMBAAGjXjBcMA4GA1UdDwEB/wQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDATAf
BgNVHSMEGDAWgBTmAVGswAlUo2ofrfN6WGf7NB57/jAUBgNVHREEDTALgglsb2Nh
bGhvc3QwDQYJKoZIhvcNAQELBQADggGBAAEasBE/Cxq74E2PzvsWWVXlWKevsUvz
HlGOhJG9LJy1gSDDw22rH2eL2qwwTWwDc6BLhKH5ZhyS3MtlhJZ9yTCyryNeeO1O
SdcnO/oUoj8NePKB7R8H2Eq2jEeZt/PPiHWz3pXbLpPoOkcM7mClTlccvmIQN/Yq
oFIGukQwtrE+UNrc93Y2vUJ24a7G2QCF1jUG5M5byyVuxAEboTWY2reW+XgZ1b5y
2/ptddgOFOv/8ZFUzcCLkVvzITmFCfclBUAsiA+ZL7jOk2xqD6Tu9N+DlHeOQ1AL
1I8OrfkohNIJrrN+uVTGDRy89wbZmRwUfpTmlMzTcKZYUPf8yj/x3GiO+rP/J5cK
F1+EqoWWI8RcyAdIVYRMR86HCs1dU45lDLUc6A3GTrAPCvJewN3U0Zl/zvJ3xfmE
NECe+t7BngRyghqnulZeiwi8QA7XY58+xT9hfOJe3Lgti0Ew+/JZ2yg9xgZzDMBz
B9sjdQkGfB/uamkY5PMjawW28m0ax1EAJg==
-----END CERTIFICATE-----`

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-client-tls"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithCertificate(cert, ""))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	result, err := client.Ping(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%++v", result)
}
