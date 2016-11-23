/*******************************************************************************
 *
 * Pentaho IoT
 *
 * Copyright (C) 2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.pentahomqttpublisher;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * MQTT Client step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
@Step( id = "MQTTPublisherMeta", image = "MQTTPublisherIcon.svg", name = "MQTT Publisher", description =
    "Publish messages to a " + "MQTT broker", categoryDescription = "Output" ) public class MQTTPublisherMeta
    extends BaseStepMeta implements StepMetaInterface {

  public static Class<?> PKG = MQTTPublisherMeta.class;

  private String broker;
  private String topic;
  private String field;
  private String clientId;
  private String timeout = "30"; // seconds according to the docs
  private String qos = "0";
  private boolean requiresAuth;
  private String username;
  private String password;
  private String sslCaFile;
  private String sslCertFile;
  private String sslKeyFile;
  private String sslKeyFilePass;

  private boolean m_topicIsFromField;

  /**
   * @return Broker URL
   */
  public String getBroker() {
    return broker;
  }

  /**
   * @param broker Broker URL
   */
  public void setBroker( String broker ) {
    this.broker = broker;
  }

  /**
   * @return MQTT topic name
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic MQTT topic name
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }

  /**
   * @param tif true if the topic is to be set from an incoming field value (topic in this case will hold the name of an
   *            incoming field instead of an absolute topic name)
   */
  public void setTopicIsFromField( boolean tif ) {
    m_topicIsFromField = tif;
  }

  /**
   * @return true if the topic is to be set from an incoming field value (topic in this case will hold the name of an
   * incoming field instead of an absolute topic name)
   */
  public boolean getTopicIsFromField() {
    return m_topicIsFromField;
  }

  /**
   * @return Target message field name in Kettle stream
   */
  public String getField() {
    return field;
  }

  /**
   * @param field Target field name in Kettle stream
   */
  public void setField( String field ) {
    this.field = field;
  }

  /**
   * @return Client ID
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * @param clientId Client ID
   */
  public void setClientId( String clientId ) {
    this.clientId = clientId;
  }

  /**
   * @return Connection timeout
   */
  public String getTimeout() {
    return timeout;
  }

  /**
   * @param timeout Connection timeout
   */
  public void setTimeout( String timeout ) {
    this.timeout = timeout;
  }

  /**
   * @return QoS to use
   */
  public String getQoS() {
    return qos;
  }

  /**
   * @param qos QoS to use
   */
  public void setQoS( String qos ) {
    this.qos = qos;
  }

  /**
   * @return Whether MQTT broker requires authentication
   */
  public boolean isRequiresAuth() {
    return requiresAuth;
  }

  /**
   * @param requiresAuth Whether MQTT broker requires authentication
   */
  public void setRequiresAuth( boolean requiresAuth ) {
    this.requiresAuth = requiresAuth;
  }

  /**
   * @return Username to MQTT broker
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username Username to MQTT broker
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * @return Password to MQTT broker
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password Password to MQTT broker
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * @return Server CA file
   */
  public String getSSLCaFile() {
    return sslCaFile;
  }

  /**
   * @param sslCaFile Server CA file
   */
  public void setSSLCaFile( String sslCaFile ) {
    this.sslCaFile = sslCaFile;
  }

  /**
   * @return Client certificate file
   */
  public String getSSLCertFile() {
    return sslCertFile;
  }

  /**
   * @param sslCertFile Client certificate file
   */
  public void setSSLCertFile( String sslCertFile ) {
    this.sslCertFile = sslCertFile;
  }

  /**
   * @return Client key file
   */
  public String getSSLKeyFile() {
    return sslKeyFile;
  }

  /**
   * @param sslKeyFile Client key file
   */
  public void setSSLKeyFile( String sslKeyFile ) {
    this.sslKeyFile = sslKeyFile;
  }

  /**
   * @return Client key file password
   */
  public String getSSLKeyFilePass() {
    return sslKeyFilePass;
  }

  /**
   * @param sslKeyFilePass Client key file password
   */
  public void setSSLKeyFilePass( String sslKeyFilePass ) {
    this.sslKeyFilePass = sslKeyFilePass;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {

    if ( broker == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidBroker" ), stepMeta ) );
    }
    if ( topic == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidTopic" ), stepMeta ) );
    }
    if ( field == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidField" ), stepMeta ) );
    }
    if ( clientId == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidClientID" ), stepMeta ) );
    }
    if ( timeout == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidConnectionTimeout" ), stepMeta ) );
    }
    if ( qos == null ) {
      remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidQOS" ), stepMeta ) );
    }
    if ( requiresAuth ) {
      if ( username == null ) {
        remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
            BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidUsername" ), stepMeta ) );
      }
      if ( password == null ) {
        remarks.add( new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
            BaseMessages.getString( PKG, "MQTTClientMeta.Check.InvalidPassword" ), stepMeta ) );
      }
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new MQTTPublisher( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new MQTTPublisherData();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {

    try {
      broker = XMLHandler.getTagValue( stepnode, "BROKER" );
      topic = XMLHandler.getTagValue( stepnode, "TOPIC" );
      String topicFromField = XMLHandler.getTagValue( stepnode, "TOPIC_IS_FROM_FIELD" );
      if ( !Const.isEmpty( topicFromField ) ) {
        m_topicIsFromField = topicFromField.equalsIgnoreCase( "Y" );
      }
      field = XMLHandler.getTagValue( stepnode, "FIELD" );
      clientId = XMLHandler.getTagValue( stepnode, "CLIENT_ID" );
      timeout = XMLHandler.getTagValue( stepnode, "TIMEOUT" );
      qos = XMLHandler.getTagValue( stepnode, "QOS" );
      requiresAuth = Boolean.parseBoolean( XMLHandler.getTagValue( stepnode, "REQUIRES_AUTH" ) );
      username = XMLHandler.getTagValue( stepnode, "USERNAME" );
      password = XMLHandler.getTagValue( stepnode, "PASSWORD" );
      if ( !Const.isEmpty( password ) ) {
        password = Encr.decryptPasswordOptionallyEncrypted( password );
      }

      Node sslNode = XMLHandler.getSubNode( stepnode, "SSL" );
      if ( sslNode != null ) {
        sslCaFile = XMLHandler.getTagValue( sslNode, "CA_FILE" );
        sslCertFile = XMLHandler.getTagValue( sslNode, "CERT_FILE" );
        sslKeyFile = XMLHandler.getTagValue( sslNode, "KEY_FILE" );
        sslKeyFilePass = XMLHandler.getTagValue( sslNode, "KEY_FILE_PASS" );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "MQTTClientMeta.Exception.loadXml" ), e );
    }
  }

  public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if ( broker != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "BROKER", broker ) );
    }
    if ( topic != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "TOPIC", topic ) );
    }

    retval.append( "    " ).
        append( XMLHandler.addTagValue( "TOPIC_IS_FROM_FIELD", m_topicIsFromField ) );

    if ( field != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "FIELD", field ) );
    }
    if ( clientId != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "CLIENT_ID", clientId ) );
    }
    if ( timeout != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "TIMEOUT", timeout ) );
    }
    if ( qos != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "QOS", qos ) );
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( "REQUIRES_AUTH", Boolean.toString( requiresAuth ) ) );

    if ( username != null ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "USERNAME", username ) );
    }
    if ( password != null ) {
      retval.append( "    " )
          .append( XMLHandler.addTagValue( "PASSWORD", Encr.encryptPasswordIfNotUsingVariables( password ) ) );
    }

    if ( sslCaFile != null || sslCertFile != null || sslKeyFile != null || sslKeyFilePass != null ) {
      retval.append( "    " ).append( XMLHandler.openTag( "SSL" ) ).append( Const.CR );
      if ( sslCaFile != null ) {
        retval.append( "      " + XMLHandler.addTagValue( "CA_FILE", sslCaFile ) );
      }
      if ( sslCertFile != null ) {
        retval.append( "      " + XMLHandler.addTagValue( "CERT_FILE", sslCertFile ) );
      }
      if ( sslKeyFile != null ) {
        retval.append( "      " + XMLHandler.addTagValue( "KEY_FILE", sslKeyFile ) );
      }
      if ( sslKeyFilePass != null ) {
        retval.append( "      " + XMLHandler.addTagValue( "KEY_FILE_PASS", sslKeyFilePass ) );
      }
      retval.append( "    " ).append( XMLHandler.closeTag( "SSL" ) ).append( Const.CR );
    }

    return retval.toString();
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
      throws KettleException {
    try {
      broker = rep.getStepAttributeString( stepId, "BROKER" );
      topic = rep.getStepAttributeString( stepId, "TOPIC" );
      m_topicIsFromField = rep.getStepAttributeBoolean( stepId, "TOPIC_IS_FROM_FIELD" );
      field = rep.getStepAttributeString( stepId, "FIELD" );
      clientId = rep.getStepAttributeString( stepId, "CLIENT_ID" );
      timeout = rep.getStepAttributeString( stepId, "TIMEOUT" );
      qos = rep.getStepAttributeString( stepId, "QOS" );
      requiresAuth = Boolean.parseBoolean( rep.getStepAttributeString( stepId, "REQUIRES_AUTH" ) );
      username = rep.getStepAttributeString( stepId, "USERNAME" );
      password = rep.getStepAttributeString( stepId, "PASSWORD" );

      sslCaFile = rep.getStepAttributeString( stepId, "SSL_CA_FILE" );
      sslCertFile = rep.getStepAttributeString( stepId, "SSL_CERT_FILE" );
      sslKeyFile = rep.getStepAttributeString( stepId, "SSL_KEY_FILE" );
      sslKeyFilePass = rep.getStepAttributeString( stepId, "SSL_KEY_FILE_PASS" );
    } catch ( Exception e ) {
      throw new KettleException( "MQTTClientMeta.Exception.loadRep", e );
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
      throws KettleException {
    try {
      if ( broker != null ) {
        rep.saveStepAttribute( transformationId, stepId, "BROKER", broker );
      }
      if ( topic != null ) {
        rep.saveStepAttribute( transformationId, stepId, "TOPIC", topic );
      }
      rep.saveStepAttribute( transformationId, stepId, "TOPIC_IS_FROM_FIELD", m_topicIsFromField );
      if ( field != null ) {
        rep.saveStepAttribute( transformationId, stepId, "FIELD", field );
      }
      if ( clientId != null ) {
        rep.saveStepAttribute( transformationId, stepId, "CLIENT_ID", clientId );
      }
      if ( timeout != null ) {
        rep.saveStepAttribute( transformationId, stepId, "TIMEOUT", timeout );
      }
      if ( qos != null ) {
        rep.saveStepAttribute( transformationId, stepId, "QOS", qos );
      }
      rep.saveStepAttribute( transformationId, stepId, "REQUIRES_AUTH", Boolean.toString( requiresAuth ) );
      if ( username != null ) {
        rep.saveStepAttribute( transformationId, stepId, "USERNAME", username );
      }
      if ( password != null ) {
        rep.saveStepAttribute( transformationId, stepId, "PASSWORD", password );
      }

      if ( sslCaFile != null ) {
        rep.saveStepAttribute( transformationId, stepId, "SSL_CA_FILE", sslCaFile );
      }
      if ( sslCertFile != null ) {
        rep.saveStepAttribute( transformationId, stepId, "SSL_CERT_FILE", sslCertFile );
      }
      if ( sslKeyFile != null ) {
        rep.saveStepAttribute( transformationId, stepId, "SSL_KEY_FILE", sslKeyFile );
      }
      if ( sslKeyFilePass != null ) {
        rep.saveStepAttribute( transformationId, stepId, "SSL_KEY_FILE_PASS", sslKeyFilePass );
      }
    } catch ( Exception e ) {
      throw new KettleException( "MQTTClientMeta.Exception.saveRep", e );
    }
  }

  public void setDefault() {
  }
}
