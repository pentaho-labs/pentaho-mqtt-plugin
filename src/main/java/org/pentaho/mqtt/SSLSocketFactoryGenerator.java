package org.pentaho.mqtt;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

public class SSLSocketFactoryGenerator {

	public static SSLSocketFactory getSocketFactory(String caCrtFile,
			String crtFile, String keyFile, String password) throws Exception {

		char[] passwordCharArray = password == null ? new char[0] : password
				.toCharArray();

		Security.addProvider(new BouncyCastleProvider());
		CertificateFactory cf = CertificateFactory.getInstance("X.509");

		X509Certificate caCert = (X509Certificate) cf
				.generateCertificate(new ByteArrayInputStream(Files
						.readAllBytes(Paths.get(caCrtFile))));

		X509Certificate cert = (X509Certificate) cf
				.generateCertificate(new ByteArrayInputStream(Files
						.readAllBytes(Paths.get(crtFile))));

		File privateKeyFile = new File(keyFile);
		PEMParser pemParser = new PEMParser(new FileReader(privateKeyFile));
		PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder()
				.build(passwordCharArray);
		JcaPEMKeyConverter converter = new JcaPEMKeyConverter()
				.setProvider("BC");

		Object object = pemParser.readObject();
		KeyPair kp;

		if (object instanceof PEMEncryptedKeyPair) {
			kp = converter.getKeyPair(((PEMEncryptedKeyPair) object)
					.decryptKeyPair(decProv));
		} else {
			kp = converter.getKeyPair((PEMKeyPair) object);
		}

		pemParser.close();

		KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		caKeyStore.load(null, null);
		caKeyStore.setCertificateEntry("ca-certificate", caCert);
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
				.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(caKeyStore);

		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		keyStore.load(null, null);
		keyStore.setCertificateEntry("certificate", cert);
		keyStore.setKeyEntry("private-key", kp.getPrivate(), passwordCharArray,
				new java.security.cert.Certificate[] { cert });
		KeyManagerFactory keyManagerFactory = KeyManagerFactory
				.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keyStore, passwordCharArray);

		// SSLContext context = SSLContext.getInstance("TLSv1");
		SSLContext context = SSLContext.getInstance("TLSv1.2");
		context.init(keyManagerFactory.getKeyManagers(),
				trustManagerFactory.getTrustManagers(), null);

		return context.getSocketFactory();

	}
}
