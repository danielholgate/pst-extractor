package com.marklogic.utilities;

import com.pff.PSTAttachment;
import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.internet.InternetHeaders;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class LoadPST {

	private static String current = System.getProperty("user.dir");

	// Minimum sizes for various mime types. Size in bytes.
	// Use this to filter out tiny .png files etc which often are embedded in line.
	// If there is no mapping here for a mime type, files of any size of that type will be saved/processed
	static Map<String,Integer> sizes = new HashMap<String,Integer>() {{
		put("image/gif", 10240);
		put("image/jpeg",10240);
		put("image/png", 10240);
	}};

	// If true de-duplicates attachments. Can increase performance for large/large number of PST files
	private static boolean DO_DEPUPLICATION = false; 
	
	private static long totalElapsedMilliseconds = 0;
	private static int folderCount = 0;
	private static int dedupeAttachmentCount = 0;
	private static int nonASCIIFolderNameCount = 0;
	private static int totalMsgCount = 0;
	private static int totalAttachmentCount = 0;
	private static int msgCount = 0;
	private static int errorCount = 0;
	private static int corruptAttachmentCount = 0;
	private static int saveFailureCount = 0;
	private static int saveCount = 0;
	private static int irrelvantAttachmentCount = 0;
	private static int dupeFolderCount = 0;
	private static int folderCreateFailedCount = 0;
	private static java.util.List<String> timings = new ArrayList<String>();
	
	static TreeMap<String,Integer> attachmentMap = new TreeMap<String,Integer>();
	static TreeMap<String,Integer> skippedAttachmentMap = new TreeMap<String,Integer>();
	static HashMap<String,String> dedupeAttachmentMap = new HashMap<String,String>();
	
	static String OUTPUT_ROOT = "";
	static String currentPSTFile = "";

	// Private class representing attachments
	// used to build List of valid file and then decide to/exclude as metadata
	private class Attachment {
		private String fileName = "";
		private String uri = "";
		
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public String getUri() {
			return uri;
		}
		public void setUri(String uri) {
			this.uri = uri;
		}
	}
	
	// PSTReport is a collection of stats about extraction from a PST file for logging
	private class PSTReport {
		
		public int getMessageCount() {
			return messageCount;
		}
		public void incMessageCount() {
			this.messageCount++;
		}
		public int getAttachmentCount() {
			return attachmentCount;
		}
		public void incAttachmentCount() {
			this.attachmentCount++;
		}
		
		public void addAttachment(String mimeType) {
			
			Integer count = 0;
			
			try {
				count = this.attachments.get(mimeType);
			} catch (Exception e) {
				//
			}
			
			count++;
			
			this.attachments.put(mimeType,count);	
		}
		
		public HashMap getAttachments() {
			return this.attachments;
		}

		private int messageCount;
		private int attachmentCount;
		private HashMap<String,Integer> attachments = new HashMap<String,Integer>();

	}

	// Main class/entry point
	public static void main(String[] args) throws IOException {
		
		if ( args.length == 2) {
			current = args[0];
			OUTPUT_ROOT = ( args[1].endsWith(File.separator) ? args[1] : args[1] + File.separator );
			System.out.println("Current dir is " + System.getProperty("user.dir") );
			System.out.println("Will load PST files from : " + current + " and extract to: " + OUTPUT_ROOT);
		//	System.out.println("Press RETURN to begin extract. Control C to abort");
		//	System.in.read();
		} else {	
			System.out.println("Please supply 2 parameters: directory-to-extract-from directory-to-extract-to");
			return;
		}

		System.out.println("Starting. Listing PST files in directory: " + current);
		listFile(current);
		ArrayList processList = new ArrayList();
		File workingDir = new File(current);

		workingDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String filename) {
				if (filename.endsWith(".pst")) return true;
				return false;
			}
		});
		
		long start = System.currentTimeMillis();

		Iterator it = processList.iterator();
		while (it.hasNext()) {
			Object obj = it.next();
			LoadPST(obj.toString());
		}
		
		long end = System.currentTimeMillis();
		long elapsedTimeMillis = end - start;
		float elapsedTimeSec = elapsedTimeMillis/1000F;

		System.out.println("===Finished. Results===");
		for (String s : timings) {
			System.out.println(s);
		}
		System.out.println("------------------------");
		System.out.println("Extracted total " + totalMsgCount + " messages in " + totalElapsedMilliseconds + "ms (= " + (totalElapsedMilliseconds/1000) + " seconds)");
		System.out.println("Total of " + totalAttachmentCount + " attachments");
		System.out.println("Total of " + folderCount + " folders");
		System.out.println("Exceptions " + errorCount);
		if ( dupeFolderCount > 0) System.out.println("Duplicate folders " + dupeFolderCount + " (No problem. attempted create with new name)");
		if ( folderCreateFailedCount > 0) System.out.println("Failed to create " + folderCreateFailedCount + " folders");
		if ( corruptAttachmentCount > 0) System.out.println("Corrupt attachment count " + corruptAttachmentCount + " (have been excluded)");
		System.out.println("Saved " + saveCount + " files");
		if ( saveFailureCount > 0) System.out.println("Save failure count " + saveFailureCount + " (Problem. Caused by invalid file name)");
		if ( irrelvantAttachmentCount > 0) System.out.println("Irrelevant attachements removed " + irrelvantAttachmentCount);
		if ( nonASCIIFolderNameCount > 0) System.out.println("Non-ASCII folder names " + nonASCIIFolderNameCount);
		if ( dedupeAttachmentCount > 0) System.out.println("Deduplicated " + dedupeAttachmentCount + " files (" + dedupeAttachmentMap.keySet().size() + " unique)");
		System.out.println("=========Included Attachments===========");
		listMap(attachmentMap);
		System.out.println("=========Skipped Attachments===========");
		listMap(skippedAttachmentMap);
		}

	public static void LoadPST(String pstFileName) {
		
		try {
			long start = System.currentTimeMillis();
			PSTFile pstFile = new PSTFile(pstFileName);
			String archiveName = pstFile.getMessageStore().getDisplayName();
			System.out.println("Processing PST file: " + archiveName);
			msgCount = 0;
			currentPSTFile = cleanFolderName(pstFile.getMessageStore().getDisplayName());
			processFolder(pstFile.getRootFolder(), cleanFolderName(getFileName(pstFileName)));
			long end = System.currentTimeMillis();
			String logMsg = "Processed PST file '" + archiveName.trim() + "' (" + getFileName(pstFileName) + "): " + msgCount + " items in " + ((end-start)/1000) + " seconds";
			totalElapsedMilliseconds = totalElapsedMilliseconds + (end-start);
			timings.add(logMsg);
			System.out.println(logMsg);
		} catch (Exception err) {
			err.printStackTrace();
			System.out.println("Exception occured in LoadPST: " + err.getMessage());
		}
	
	}
	
	private static String getFileName(String filePath) {
		Path p = Paths.get(filePath);
		String file = p.getFileName().toString();
		return file;
	}
	
	private static String removeUnicode(String name) {
	     String cleanName = name.replaceAll("[^\\x00-\\x7F]", "");
	     return cleanName;
	}
	
	// Clean up folder name to remove potential problem characters (forward slash, leading/trailing spaces etc)
	private static String cleanFolderName(String folderName) {
		
		//String cleanName = folderName.replaceAll("[^\\x00-\\x7F]", "");
		String cleanName = folderName.trim();
		cleanName = cleanName.replaceAll("\\s+", "_");
		cleanName = cleanName.replaceAll("_-_", "-");
		cleanName = cleanName.replaceAll(":", "_");
		cleanName = cleanName.replaceAll("_+", "_");
		cleanName = cleanName.replaceAll("-+", "-");
		
		return cleanName;
	}
	
	private static String cleanUnixFileName(String fileName) {
		
       String cleanName = fileName.replaceAll("/", "-").trim();
       cleanName = cleanName.replaceAll("\\s+"," ");
	   return cleanName;
	}

	public static void processFolder(PSTFolder pstFolder, String parentFolderName)
			throws PSTException, IOException, ParseException {
		
		String testUnicode = pstFolder.getDisplayName();
		if ( ! testUnicode.equals(pstFolder.getDisplayName())) {
			System.out.println("Non ASCII folder name found: " + pstFolder.getDisplayName() + " (after was " + testUnicode);
			nonASCIIFolderNameCount++;
		}
		
		System.out.println("processFolder("+ pstFolder.getDisplayName() + "," + parentFolderName + ")" );
		folderCount++;
		int subFolderCount = 0;
		try {
			subFolderCount = pstFolder.getSubFolderCount();
		} catch (Exception e) {
			// swallow exception. Some folders (like Search folders) throw an exception when you try to retrieve child folders
		}
		System.out.println("Processing PST folder: " + pstFolder.getDisplayName() + (subFolderCount > 0 ? " (has " + subFolderCount + " subfolders)" : ""));
		if ( pstFolder.getDisplayName().trim().length() == 0) {
			parentFolderName = currentPSTFile + "_pst-Folders";
			System.out.println("Folder name empty. Using " + parentFolderName );
		}
		
		if (pstFolder.hasSubfolders()) {
			Vector<PSTFolder> childFolders = pstFolder.getSubFolders();
			for (PSTFolder childFolder : childFolders) {
				processFolder(childFolder, parentFolderName + File.separator + cleanFolderName(pstFolder.getDisplayName()));
			}

		}

		if (pstFolder.getContentCount() > 0) {
			System.out.println(
					"Folder '" + pstFolder.getDisplayName() + "' contains " + pstFolder.getContentCount() + " items");
			PSTMessage email = (PSTMessage) pstFolder.getNextChild();

			String folderName = cleanFolderName( pstFolder.getDisplayName() );	

			while (email != null) {
				
				String messagePath = "";
				
				try {
					
					totalMsgCount++;
					msgCount++;
					int bodySize = email.getBody() != null ? email.getBody().length() : 0;
					System.out.println("Processing email #" + msgCount + "  '" + email.getSubject() + "' Body size: "+ bodySize + " bytes");
					messagePath = OUTPUT_ROOT + "/email/" + parentFolderName + File.separator + folderName + File.separator + "message-" + msgCount;
					String attachmentPath = OUTPUT_ROOT + "/attachments/" + parentFolderName + File.separator + folderName + File.separator + "message-" + msgCount + "/attachments/";

					messagePath = messagePath.replaceAll("/+", File.separator).trim();
					attachmentPath = attachmentPath.replaceAll("/+", File.separator).trim();
					
					System.out.println("  creating msg dir: " + messagePath);
					System.out.println("  creating attachment dir: " + attachmentPath);

					boolean createMsgDirCheck = new File(messagePath).mkdirs();
					boolean createAttachDirCheck = new File(attachmentPath).mkdirs();
					
					if ( ! createAttachDirCheck || ! createMsgDirCheck ) {
						
						if ( ! createAttachDirCheck ) System.out.println("FAILED TO CREATE ATTACHMENT DIRECTORY: " + attachmentPath);
						if ( ! createMsgDirCheck ) System.out.println("FAILED TO CREATE MESSAGE DIRECTORY: " + messagePath);

						boolean exists = new File(attachmentPath).exists();
						if ( exists) dupeFolderCount++;
						System.out.println("FAILED TO CREATE DIRECTORY: " + attachmentPath + ( exists ? "already exists" : "DOESN'T ALREADY EXIST" ));
						messagePath = OUTPUT_ROOT + "/email/" + parentFolderName + File.separator + folderName + "-2" + File.separator + "message-" + msgCount;
						attachmentPath = OUTPUT_ROOT + "/attachments/" + parentFolderName + File.separator + folderName + "-2" + File.separator + "message-" + msgCount + "/attachments/";
						createAttachDirCheck = new File(messagePath + File.separator + "attachments" + File.separator).mkdirs();
						 if ( ! createAttachDirCheck ) { 
							 System.out.println("SECOND ATTEMPT TO CREATE ATTACHMENT PATH FAILED: " + attachmentPath); 
							 folderCreateFailedCount++;
						 }
						 else { System.out.println("CREATED: " + messagePath + File.separator + "attachments instead"); }
					}
					
					// Remove any double //
					messagePath = messagePath.replaceAll("/+", File.separator).trim();
					attachmentPath = attachmentPath.replaceAll("/+", File.separator).trim();

					DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
					DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

					Document doc = docBuilder.newDocument();
					Element rootElement = doc.createElement("pst-extract");
					doc.appendChild(rootElement);

					Element pst_extract = doc.createElement("message");
					rootElement.appendChild(pst_extract);

					Element path = doc.createElement("path");
					path.appendChild(doc.createTextNode(pstFolder.getDisplayName()));
					pst_extract.appendChild(path);

					Element attachmentTag = doc.createElement("attachments");

					if (email.hasAttachments()) {

						java.util.List<Attachment> atch = parseAndSaveAttachments(email, attachmentPath);
						
						attachmentTag.appendChild(doc.createTextNode(String.valueOf(atch.size())));
						pst_extract.appendChild(attachmentTag);

						for (Attachment a : atch) {

							Element attach_details = doc.createElement("attachments-details");
							attach_details.appendChild(doc.createTextNode(a.getFileName()));
							pst_extract.appendChild(attach_details);

							Element doc_location = doc.createElement("doc-location");
							doc_location.appendChild(doc.createTextNode(a.getUri()));
							pst_extract.appendChild(doc_location);

						}	

					} else {
						attachmentTag.appendChild(doc.createTextNode(String.valueOf(0)));
						pst_extract.appendChild(attachmentTag);
					}


					Element Intheaders = doc.createElement("headers");

					Enumeration allHeaders = null;
					try {
						InternetHeaders headers = new InternetHeaders(
								new ByteArrayInputStream(email.getTransportMessageHeaders().getBytes()));
						allHeaders = headers.getAllHeaders();
					} catch (MessagingException e) {
						e.printStackTrace();
					}

					while (allHeaders.hasMoreElements()) {
						Header header = (Header) allHeaders.nextElement();
						if (header.getName().equalsIgnoreCase("Content-Type")) {
							Element ct = doc.createElement("Content-Type");
							ct.appendChild(doc.createTextNode(header.getValue().trim()));
							Intheaders.appendChild(ct);
						}
						if (header.getName().equalsIgnoreCase("Content-Transfer-Encoding")) {
							Element cte = doc.createElement("Content-Transfer-Encoding");
							cte.appendChild(doc.createTextNode(header.getValue().trim()));
							Intheaders.appendChild(cte);
						}
						if (header.getName().equalsIgnoreCase("To")) {
							Element ht = doc.createElement("Header-To");
							ht.appendChild(doc.createTextNode(header.getValue().trim()));
							Intheaders.appendChild(ht);
						}
						pst_extract.appendChild(Intheaders);
					}

					Element from = doc.createElement("from");
					from.appendChild(doc.createTextNode(email.getSenderName().trim()));
					pst_extract.appendChild(from);

					Element to = doc.createElement("to");
					to.appendChild(doc.createTextNode(email.getDisplayTo().trim()));
					pst_extract.appendChild(to);

					Element subject = doc.createElement("subject");
					subject.appendChild(doc.createTextNode(email.getSubject().trim()));
					pst_extract.appendChild(subject);

					Element sent = doc.createElement("sent");
					if (email.getClientSubmitTime() != null) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						String date = sdf.format(email.getClientSubmitTime());

						sent.appendChild(doc.createTextNode(date.toString().trim()));
						pst_extract.appendChild(sent);
					}
					Element sentTS = doc.createElement("sent-timestamp");
					if (email.getClientSubmitTime() != null) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
						String date = sdf.format(email.getClientSubmitTime());
						sentTS.appendChild(doc.createTextNode(date.toString().trim()));
						pst_extract.appendChild(sentTS);
					}

					Element count = doc.createElement("audience_count");
					count.appendChild(doc.createTextNode(String.valueOf(email.getNumberOfRecipients())));
					pst_extract.appendChild(count);

					Element body = doc.createElement("body");
					body.appendChild(doc.createTextNode(
							email.getBody().trim().replace('\r', ' ').replace('\'', ' ').replaceAll("&#1;", " ")));
					pst_extract.appendChild(body);

					TransformerFactory transformerFactory = TransformerFactory.newInstance();
					Transformer transformer = transformerFactory.newTransformer();
					DOMSource source = new DOMSource(doc);

					StreamResult result = new StreamResult(messagePath + File.separator + "message-" + msgCount + ".xml");
					transformer.setOutputProperty("indent", "yes");
					transformer.transform(source, result);

				} catch (ParserConfigurationException pce) {
					pce.printStackTrace();
					errorCount++;
				} catch (TransformerException tfe) {
					tfe.printStackTrace();
					errorCount++;
				}

				email = (PSTMessage) pstFolder.getNextChild();
			}
			System.out.println("INFO: " + errorCount + " exceptions occurred processing " + pstFolder.getContentCount()
					+ " content items");
		}
		System.out.println("Finished processing folder: " + pstFolder.getDisplayName());
	}

	public static void listFile(String pathname) {
		File f = new File(pathname);
		File[] listfiles = f.listFiles();
		for (int i = 0; i < listfiles.length; i++)
			if (listfiles[i].isDirectory()) {
				File[] internalFile = listfiles[i].listFiles();
				for (int j = 0; j < internalFile.length; j++) {
					if (internalFile[j].toString().endsWith(".pst")) {
						System.out.println("Now processing " + internalFile[j]);
						LoadPST(internalFile[j].toString());
					}
					if (internalFile[j].isDirectory()) {
						String name = internalFile[j].getAbsolutePath();
						listFile(name);
					}
				}
			} else if (listfiles[i].getAbsoluteFile().toString().endsWith(".pst")) {
				LoadPST(listfiles[i].toString());
			}
	}
	
	/*
	 * Returns true if attachment should be included in extract
	 * Based on sizes for various MIME types in sizes
	 */
	private static boolean isRelevantAttachment(PSTAttachment at) {
		
		int size = at.getAttachSize();
		boolean isRelevant = true;
		
		int validSize = 0;
		String atchMime = "";
		
		try {
			 atchMime = at.getMimeTag();
		} catch (Exception e) {
			
		}
		
		
		try {
		 if ( atchMime != null && atchMime.length() > 0 ) validSize = sizes.get(atchMime);
		if ( validSize > 0 ) {
			if ( size < validSize ) isRelevant = false;
		}
		} catch (Exception e) {
			// Swallow exception. just means the given mime type had no mapping. we will accept/process any size of this type
		}
		
		return isRelevant;	
	}

	/*
	 * Parse attachments for an email and return a List of objects with details of each
	 */
	private static java.util.List<Attachment> parseAndSaveAttachments(PSTMessage email, String attachmentPath) {

		System.out.println("Has " + email.getNumberOfAttachments() + " attachments");
		String attachmentFileName = "";
		ArrayList<Attachment> attachmentList = new ArrayList<Attachment>();

		Path p = Paths.get(attachmentPath);
		if (! Files.exists(p) ) {
			try {
			Files.createDirectories(p);
			} catch (Exception e) {
				System.out.println("Failed to create directory " + p.toString());
				errorCount++;
			}
		}

		for (int x = 0; x < email.getNumberOfAttachments(); x++) {
			try {
				
				PSTAttachment attachment = email.getAttachment(x);
				totalAttachmentCount++;
				
				String mimeType = "";
				try { 
					mimeType = attachment.getMimeTag();
				} catch (Exception e) {
					mimeType = "";
				}
				
				if ( mimeType == "" ||  mimeType.length() == 0 ) {
					
					String name = attachment.getDisplayName();
					if ( name != null && name.length() > 0) {
						
					String[] parts =  name.trim().split("\\.");
					String extension = parts[parts.length-1];

					// Auto-repair mime types
					switch (extension) {
		            	case "pdf":  mimeType = "application/pdf";      
		            	break;
		            	case "pptx":  mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation";        
		            	break;
		            	case "ppt":  mimeType = "application/vnd.openxmlformats-officedocument.presentationml.presentation";
                    	break;
		            	case "htm":  mimeType = "text/html";
                    	break;
		            	case "html":  mimeType = "text/html";
                    	break;
		            	case "xls":  mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                    	break;
		            	case "xlsx":  mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                    	break;
		            	case "txt":  mimeType = "text/plain";
                    	break;
		            	case "csv":  mimeType = "text/csv";
                    	break;
		            	case "docx":  mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                    	break;
		            	case "doc":  mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                    	break;
		            	default: mimeType = "";
		            	break;
					}
					System.out.println("Auto-assigned MIME type " + mimeType + " to " + name.trim());
					}
				}

				if ( mimeType == "" ||  mimeType.length() == 0 ) {
					System.out.println("Could not auto-assign MIME type for attachment '" + attachment.getDisplayName() + " has empty MIME type.");
				}
				
				recordAttachementType(attachmentMap,mimeType);

				if (!isRelevantAttachment(attachment)) {
					System.out.println("Skipping " + attachment.getDisplayName() + " (" + mimeType + ") " + attachment.getAttachSize() + " bytes");
					irrelvantAttachmentCount++;
					recordAttachementType(skippedAttachmentMap,mimeType);
					continue;
				} 

				if (attachment.getFilename() != null && attachment.getFilename().length() > 0) {
					attachmentFileName = attachment.getFilename();
				} else {
					attachmentFileName = "emptyNameAttachment" + x;
				}
				
				attachmentFileName = cleanUnixFileName(attachmentFileName);

				if (fileExists(attachmentPath + attachmentFileName)) {
					attachmentFileName = x + "-" + attachmentFileName;
				}

				InputStream attachmentStream = attachment.getFileInputStream();
				
				boolean attachmentWriteOk = false;
				
				// Check if this file is already in the de-dupe list
				String dedupeURI = getDedupe(attachment);
				if ( (dedupeURI == "" && DO_DEPUPLICATION) || ! DO_DEPUPLICATION ) {
					
				// Save attachment
				try {
					attachmentWriteOk = writeToFile(attachmentStream, attachmentPath, attachmentFileName, attachment, mimeType);
					attachmentStream.close();
					addDedupe(attachment,(attachmentPath + attachmentFileName).replaceAll("/+", File.separator));
				} catch (IOException ioe) {
					System.out.println("Exception saving " + attachmentFileName + "(" + ioe.toString() + ")");
				} finally {
					if ( attachmentStream != null) {
						try { attachmentStream.close(); }
						catch (IOException ioe) {
							System.out.println("Ignore. Exception closing input stream " + ioe.toString() );
						}
					}
				}
				
				} else {
					if (DO_DEPUPLICATION) {
						System.out.println("Deduplicated " + attachment.getDisplayName());
						dedupeAttachmentCount++;
					}
					attachmentWriteOk = true;
				}

				if (attachmentWriteOk) { // Add to list to be added as metadata in message XML
					
					String uri = "";
					
					if ( dedupeURI.length() > 0 ) {
						uri = dedupeURI;
					} else {
						uri = attachmentPath + attachmentFileName;
						uri = uri.replaceAll("/+", File.separator);
					}

					Attachment a = new LoadPST().new Attachment();
					a.setFileName(attachmentFileName);	
					//String uri = attachmentPath + attachmentFileName;
					//uri = uri.replaceAll("/+", File.separator);
					a.setUri(uri);
					attachmentList.add(a);

				} else { // Delete bad file so MLCP does not try to ingest it, also do not add as metadata
					deleteCorruptFile(attachmentPath, attachmentFileName);
					recordAttachementType(skippedAttachmentMap,mimeType);
				}

			} catch (IOException | PSTException ioe) {
				System.out.println("Exception getting input stream for " + attachmentFileName + "(" + ioe.toString() + ")");
			}
		}

		return attachmentList;
	}

	private static boolean fileExists(String filePathString) throws IOException {
		File f = new File(filePathString);
		return f.exists();
	}

	private static void deleteCorruptFile(String attachmentPath, String fileName) {
		File f = new File(attachmentPath + fileName);
		boolean success = f.delete();
		System.out.println(
				"Skipping corrupt file " + attachmentPath + fileName + " : " + (success ? "DELETED" : "DELETE FAILED"));
	}

	private static boolean writeToFile(InputStream stream, String attachmentPath, String fileName,
			PSTAttachment atchmnt, String mimeType) throws IOException {

		StringBuffer sb1 = new StringBuffer(attachmentPath + fileName);
		float origSize = Float.valueOf(atchmnt.getAttachSize());
		System.out.println("  Saving attachment '" + fileName.trim() + "' (" + mimeType + ","
				+ atchmnt.getAttachSize() + " bytes)");
		FileChannel outChannel = null;
		ReadableByteChannel inChannel = null;
		boolean fileIntact = true;

		try {

			outChannel = new FileOutputStream(sb1.toString().trim()).getChannel();
			inChannel = Channels.newChannel(stream);
			ByteBuffer buffer = ByteBuffer.allocate(1024);

			while (inChannel.read(buffer) != -1) {
				buffer.flip();
				outChannel.write(buffer);
				buffer.clear();
			}
			
			saveCount++;
	
		} catch (IOException e) {
			System.out.println("Exception occurred saving '" + attachmentPath + fileName + "' (" + e.toString() + ")");
			saveFailureCount++;
		} finally {
			if ( inChannel != null ) inChannel.close();
			if ( outChannel != null ) outChannel.close();
			File f = new File(attachmentPath + fileName);
			float sizeDiff = (f.length() / origSize) * 100;
			if (f.length() == 8 && origSize != 8) {
				System.out.println("Size on disk is " + f.length() + " bytes (" + sizeDiff + "% of report PST size). Attachment corrupt: will not add");
				fileIntact = false;
				corruptAttachmentCount++;
			}
		}
		return fileIntact;
	}
	
	private static void recordAttachementType(Map<String,Integer> map, String attachType) {
		
		String a = attachType.toLowerCase();
		
		if ( ! map.containsKey(a) ) {
			map.put(attachType, 1);
		} else {
			int count = map.get(a);
			count++;
			map.put(a, count);
		}
	}
	
	/*
	 * De-duplication helper methods
	 */
	
	// Add file to list if it doesn't already exist
	// ..Uses file name and size to build a hash. Not perfect, but good enough.
	private static void addDedupe(PSTAttachment atchmnt, String uri) {
		String key = "";
		try {
			key = atchmnt.getDisplayName().trim() + atchmnt.getAttachSize();
			if (key != null || key.length() > 0) dedupeAttachmentMap.put(key,uri);
		} catch (Exception e) {
			key = "";
		}
	}
	
	// Return path to file if already in de-dupe list
	private static String getDedupe(PSTAttachment atchmnt) {
		String key, uri = "";
		try {
			key = atchmnt.getDisplayName().trim() + atchmnt.getAttachSize();
			if (key != null || key.length() > 0) uri = dedupeAttachmentMap.get(key);
			if ( uri == null) uri = "";
		} catch (Exception e) {
			uri = "";
		}
		return uri;
	}
	
	// Helper method to pretty print a map
	private static void listMap(Map<String,Integer> map) {
	
	Iterator entries = map.entrySet().iterator();
	while (entries.hasNext()) {
	  Entry thisEntry = (Entry) entries.next();
	  Object key = thisEntry.getKey();
	  Object value = thisEntry.getValue();
	  System.out.println(key + " : " + value);
	}
	}

}