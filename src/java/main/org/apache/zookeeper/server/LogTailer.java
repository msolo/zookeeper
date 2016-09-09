/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.persistence.FileHeader;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;


public class LogTailer {
    private static final Logger LOG = LoggerFactory.getLogger(LogFormatter.class);

    public static String formatTxn(TxnHeader hdr, Record txn) {
        switch (hdr.getType()) {
        case OpCode.createSession:
            // This isn't really an error txn; it just has the same
            // format. The error represents the timeout
            CreateSessionTxn cstxn = (CreateSessionTxn)txn;
            return "timeOut:" + cstxn.getTimeOut();
        case OpCode.closeSession:
            return "";
        case OpCode.create:
        case OpCode.create2:
            CreateTxn ctxn = (CreateTxn)txn;
            return "path:" + ctxn.getPath() + " len:" + ctxn.getData().length;
        case OpCode.createContainer:
            CreateContainerTxn cctxn = (CreateContainerTxn)txn;
            return "path:" + cctxn.getPath() + " len:" + cctxn.getData().length;
        case OpCode.delete:
        case OpCode.deleteContainer:
            DeleteTxn dtxn = (DeleteTxn)txn;
            return "path:" + dtxn.getPath();
        case OpCode.reconfig:
        case OpCode.setData:
            SetDataTxn stxn = (SetDataTxn)txn;
            return "path:" + stxn.getPath() + " len:" + stxn.getData().length;
        case OpCode.error:
            ErrorTxn etxn = (ErrorTxn)txn;
            return "err:" + etxn.getErr();
        case OpCode.multi:
            MultiTxn mtxn = (MultiTxn)txn;
            return "len:" + mtxn.getTxns().size();
        default:
            return "unknown txn type" + hdr.getType();
        }
    }
    
    public static String formatTxnData(TxnHeader hdr, Record txn) {
        byte[] data = null;
        switch (hdr.getType()) {
        case OpCode.createSession:
        case OpCode.closeSession:
        case OpCode.delete:
        case OpCode.deleteContainer:
        case OpCode.error:
            return null;
        case OpCode.create:
        case OpCode.create2:
            CreateTxn ctxn = (CreateTxn)txn;
            data = ctxn.getData();
            break;
        case OpCode.createContainer:
            CreateContainerTxn cctxn = (CreateContainerTxn)txn;
            data = cctxn.getData();
            break;
        case OpCode.reconfig:
        case OpCode.setData:
            SetDataTxn stxn = (SetDataTxn)txn;
            data = stxn.getData();
            break;
        case OpCode.multi:
            MultiTxn mtxn = (MultiTxn)txn;
            data = ("multi txn:" + mtxn.getTxns().size()).getBytes();
            break;
        default:
            return "unknown txn type" + hdr.getType();
        }
        try {
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return Base64.getEncoder().encodeToString(data);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1 && args.length != 2) {
            System.err.println("USAGE: LogTailer [-v] <log_file_name>");
            System.exit(2);
        }
        boolean verbose = false;
        boolean showData = false;
        String fname = "";
        for (int i=0; i<args.length; i++) {
            if (args[i].equals("--verbose")) {
                verbose = true;
            } else if (args[i].equals("--show-data")) {
                showData = true;
            } else {
                fname = args[i];
            }
        }        
        
        RandomAccessFile raf = new RandomAccessFile(fname, "r");
        BinaryInputArchive logStream = new BinaryInputArchive(raf);
        FileHeader fhdr = new FileHeader();
        fhdr.deserialize(logStream, "fileheader");

        if (fhdr.getMagic() != FileTxnLog.TXNLOG_MAGIC) {
            System.err.println("Invalid magic number for " + fname);
            System.exit(2);
        }
        System.out.println("ZooKeeper Transactional Log File with dbid "
                + fhdr.getDbid() + " txnlog format version "
                + fhdr.getVersion());

        DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // Quoted "Z" to indicate UTC, no timezone offset
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        int count = 0;
        while (true) {
            long crcValue;
            byte[] bytes;
            long offset = 0;
            try {
                offset = raf.getFilePointer();
                crcValue = logStream.readLong("crcvalue");
                bytes = logStream.readBuffer("txnEntry");
            } catch (EOFException e) {
                System.out.println("EOF reached after " + count + " txns.");
                return;
            }
            if (bytes.length == 0) {
                // Since we preallocate, we define EOF to be an
                // empty transaction
                long sleepMs = 500;
                if (verbose) {
                    System.out.println("EOF reached after " + count + " txns. Resetting.");
                    sleepMs = 5000;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch(InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                raf.seek(offset);
                logStream = new BinaryInputArchive(raf);
                continue;
            }
            Checksum crc = new Adler32();
            crc.update(bytes, 0, bytes.length);
            if (crcValue != crc.getValue()) {
                throw new IOException("CRC doesn't match " + crcValue +
                        " vs " + crc.getValue());
            }
            TxnHeader hdr = new TxnHeader();
            Record txn = SerializeUtils.deserializeTxn(bytes, hdr);
            String txnStr = formatTxn(hdr, txn);
            String timestamp = dateFmt.format(new Date(hdr.getTime()));
            long millis = hdr.getTime() % 1000;
            System.out.println(timestamp + "." + millis + "Z"
                    + " session:0x"
                    + Long.toHexString(hdr.getClientId())
                    + " cxid:0x"
                    + Long.toHexString(hdr.getCxid())
                    + " zxid:0x"
                    + Long.toHexString(hdr.getZxid())
                    + " " + TraceFormatter.op2String(hdr.getType()) + " " + txnStr);
            if (showData) {
                String data = formatTxnData(hdr, txn);
                if (data != null) {
                    System.out.println(data);
                }
            }
            if (verbose) {
                System.out.println(txn);
            }
            if (logStream.readByte("EOR") != 'B') {
                LOG.error("Last transaction was partial.");
                throw new EOFException("Last transaction was partial.");
            }
            count++;
        }
    }
}
