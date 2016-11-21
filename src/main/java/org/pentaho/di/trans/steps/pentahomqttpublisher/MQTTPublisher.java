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

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.mqtt.SSLSocketFactoryGenerator;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * MQTT m_client step publisher
 *
 * @author Michael Spector
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MQTTPublisher extends BaseStep implements StepInterface {

  public MQTTPublisher( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    MQTTPublisherData data = (MQTTPublisherData) sdi;

    shutdown( data );
    super.dispose( smi, sdi );
  }

  protected void configureConnection( MQTTPublisherMeta meta, MQTTPublisherData data ) throws KettleException {
    if ( data.m_client == null ) {
      String broker = environmentSubstitute( meta.getBroker() );
      if ( Const.isEmpty( broker ) ) {
        throw new KettleException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoBrokerURL" ) );
      }
      String clientId = environmentSubstitute( meta.getClientId() );
      if ( Const.isEmpty( clientId ) ) {
        throw new KettleException( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Error.NoClientID" ) );
      }

      try {
        data.m_client = new MqttClient( broker, clientId );

        MqttConnectOptions connectOptions = new MqttConnectOptions();
        if ( meta.isRequiresAuth() ) {
          connectOptions.setUserName( environmentSubstitute( meta.getUsername() ) );
          connectOptions.setPassword( environmentSubstitute( meta.getPassword() ).toCharArray() );
        }
        if ( broker.startsWith( "ssl:" ) || broker.startsWith( "wss:" ) ) {
          connectOptions.setSocketFactory( SSLSocketFactoryGenerator
              .getSocketFactory( environmentSubstitute( meta.getSSLCaFile() ),
                  environmentSubstitute( meta.getSSLCertFile() ), environmentSubstitute( meta.getSSLKeyFile() ),
                  environmentSubstitute( meta.getSSLKeyFilePass() ) ) );
        }
        connectOptions.setCleanSession( true );

        String timeout = environmentSubstitute( meta.getTimeout() );
        try {
          connectOptions.setConnectionTimeout( Integer.parseInt( timeout ) );
        } catch ( NumberFormatException e ) {
          throw new KettleException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongTimeoutValue.Message", timeout ), e );
        }

        logBasic( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.CreateMQTTClient.Message", broker, clientId ) );
        data.m_client.connect( connectOptions );

      } catch ( Exception e ) {
        throw new KettleException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorCreateMQTTClient.Message", broker ),
            e );
      }
    }
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow();
    if ( r == null ) {
      setOutputDone();
      return false;
    }

    MQTTPublisherMeta meta = (MQTTPublisherMeta) smi;
    MQTTPublisherData data = (MQTTPublisherData) sdi;

    RowMetaInterface inputRowMeta = getInputRowMeta();

    if ( first ) {
      first = false;

      // Initialize MQTT m_client:
      configureConnection( meta, data );

      data.m_outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.m_outputRowMeta, getStepname(), null, null, this );

      String inputField = environmentSubstitute( meta.getField() );

      int numErrors = 0;
      if ( Const.isEmpty( inputField ) ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.FieldNameIsNull" ) ); //$NON-NLS-1$
        numErrors++;
      }
      data.m_inputFieldNr = inputRowMeta.indexOfValue( inputField );
      if ( data.m_inputFieldNr < 0 ) {
        logError( BaseMessages
            .getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.CouldntFindField", inputField ) ); //$NON-NLS-1$
        numErrors++;
      }

      if ( numErrors > 0 ) {
        setErrors( numErrors );
        stopAll();
        return false;
      }
      data.m_inputFieldMeta = inputRowMeta.getValueMeta( data.m_inputFieldNr );
      data.m_topic = environmentSubstitute( meta.getTopic() );
      if ( meta.getTopicIsFromField() ) {
        data.m_topicFromFieldIndex = inputRowMeta.indexOfValue( data.m_topic );
        if ( data.m_topicFromFieldIndex < 0 ) {
          throw new KettleException(
              "Incoming stream does not seem to contain the topic field '" + data.m_topic + "'" );
        }

        if ( inputRowMeta.getValueMeta( data.m_topicFromFieldIndex ).getType() != ValueMetaInterface.TYPE_STRING ) {
          throw new KettleException( "Incoming stream field to use for setting the topic must be of type string" );
        }
      }

      String qosValue = environmentSubstitute( meta.getQoS() );
      try {
        data.m_qos = Integer.parseInt( qosValue );
        if ( data.m_qos < 0 || data.m_qos > 2 ) {
          throw new KettleException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ) );
        }
      } catch ( NumberFormatException e ) {
        throw new KettleException(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.WrongQOSValue.Message", qosValue ), e );
      }
    }

    try {
      if ( !isStopped() ) {
        Object rawMessage = r[data.m_inputFieldNr];
        byte[] message = messageToBytes( rawMessage, data.m_inputFieldMeta );
        if ( message == null ) {
          logDetailed( "Incoming message value is null/empty - skipping" );
          return true;
        }

        // String topic = environmentSubstitute( meta.getTopic() );

        if ( meta.getTopicIsFromField() ) {
          if ( r[data.m_topicFromFieldIndex] == null || Const.isEmpty( r[data.m_topicFromFieldIndex].toString() ) ) {
            // TODO add a default topic option, and then only skip if the default is null
            logDetailed( "Incoming topic value is null/empty - skipping message: " + rawMessage );
            return true;
          }
          data.m_topic = r[data.m_topicFromFieldIndex].toString();
        }

        MqttMessage mqttMessage = new MqttMessage( message );
        mqttMessage.setQos( data.m_qos );

        logBasic( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.Log.SendingData", data.m_topic,
            Integer.toString( data.m_qos ) ) );
        if ( isRowLevel() ) {
          logRowlevel( data.m_inputFieldMeta.getString( r[data.m_inputFieldNr] ) );
        }
        try {
          data.m_client.publish( data.m_topic, mqttMessage );
        } catch ( MqttException e ) {
          throw new KettleException(
              BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorPublishing.Message" ), e );
        }
      }
    } catch ( KettleException e ) {
      if ( !getStepMeta().isDoingErrorHandling() ) {
        logError(
            BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorInStepRunning", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone();
        return false;
      }
      putError( getInputRowMeta(), r, 1, e.toString(), null, getStepname() );
    }
    return true;
  }

  protected void shutdown( MQTTPublisherData data ) {
    if ( data.m_client != null ) {
      try {
        if ( data.m_client.isConnected() ) {
          data.m_client.disconnect();
        }
        data.m_client.close();
        data.m_client = null;
      } catch ( MqttException e ) {
        logError( BaseMessages.getString( MQTTPublisherMeta.PKG, "MQTTClientStep.ErrorClosingMQTTClient.Message" ), e );
      }
    }
  }

  public void stopRunning( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    MQTTPublisherData data = (MQTTPublisherData) sdi;
    shutdown( data );
    super.stopRunning( smi, sdi );
  }

  protected byte[] messageToBytes( Object message, ValueMetaInterface messageValueMeta ) throws KettleValueException {
    if ( message == null || Const.isEmpty( message.toString() ) ) {
      return null;
    }

    byte[] result = null;
    try {
      ByteBuffer buff = null;
      switch ( messageValueMeta.getType() ) {
        case ValueMetaInterface.TYPE_STRING:
          result = message.toString().getBytes( "UTF-8" );
          break;
        case ValueMetaInterface.TYPE_INTEGER:
        case ValueMetaInterface.TYPE_DATE: // send the date as a long (milliseconds) value
          buff = ByteBuffer.allocate( 8 );
          buff.putLong( messageValueMeta.getInteger( message ) );
          result = buff.array();
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          buff = ByteBuffer.allocate( 8 );
          buff.putDouble( messageValueMeta.getNumber( message ) );
          result = buff.array();
          break;
        case ValueMetaInterface.TYPE_TIMESTAMP:
          buff = ByteBuffer.allocate( 12 );
          Timestamp ts = (Timestamp) message;
          buff.putLong( ts.getTime() );
          buff.putInt( ts.getNanos() );
          result = buff.array();
          break;
        case ValueMetaInterface.TYPE_BINARY:
          result = messageValueMeta.getBinary( message );
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          result = new byte[1];
          if ( messageValueMeta.getBoolean( message ) ) {
            result[0] = 1;
          }
          break;
        case ValueMetaInterface.TYPE_SERIALIZABLE:
          if ( !( message instanceof Serializable ) ) {
            throw new KettleValueException( "Message value is not serializable!" );
          }
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream( bos );
          oos.writeObject( message );
          oos.flush();
          result = bos.toByteArray();
          break;
      }
    } catch ( Exception ex ) {
      throw new KettleValueException( ex );
    }

    return result;
  }
}
