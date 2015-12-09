package iie.SparkStreaming;

import iie.udps.api.streaming.DStreamWithSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class OpContainer {
	static Map<String, DStreamWithSchema> RESULT_MAP = new HashMap<String, DStreamWithSchema>();
	static String START_LOG = "";

	public static void main(String[] args) throws Exception {
		JavaStreamingContext jssc = new JavaStreamingContext(
				new SparkConf().setAppName("SparkStreamingOperatorTest"),
				new Duration(10000));

		String xmlPath = "/home/dingyu/xml/procedureDefinitionTest.xml";
		if (args.length > 1)
			xmlPath = args[1];

		// 解析算子连接关系图，使用HashMap存储图结构
		HashMap<String, OpNode> opGraphMap = parsingOpFlow(xmlPath);

		// 拓扑排序

		List<OpNode> topologicalOrder = topologicalOrder(opGraphMap);

		// 反射机制拓扑顺序执行算子
		executeOperator(jssc, topologicalOrder);

		writeStartupLog(START_LOG, xmlPath);

		jssc.start();
		jssc.awaitTermination();
	}

	public static void writeStartupLog(String log, String xmlPath) {
		Document document = readXMLFromHDFS(xmlPath);
		Element rootNode = document.getRootElement();
		String jobInstanceId = rootNode.element("jobinstanceid").getTextTrim();
		List<Element> propertys = document
				.selectNodes("/requestParams/operator/context/property");
		String tempHDFSPathValue = "";
		for (Element property : propertys) {
			if ("tempHdfsBasePath".equals(property.attributeValue("name"))) {
				tempHDFSPathValue = property.attributeValue("value");
				if (!"/".equals(tempHDFSPathValue.charAt(tempHDFSPathValue
						.length() - 1))) {
					tempHDFSPathValue += "/";
				}
			}
		}
		Map<String, String> testMap = new HashMap<String, String>();
		testMap.put("startLog", log);
		writeToHDFSFile(tempHDFSPathValue+"startLog-"+jobInstanceId+".xml", testMap);
	}

	public static void executeOperator(JavaStreamingContext jssc,
			List<OpNode> topologicalOrder) throws MalformedURLException {
		String outputMessage = "";
		for (OpNode operator : topologicalOrder) {

			System.out.println("**************************\n running "
					+ operator.getOpMainClassName());

			String arguments = operator.getArgsXML();
			List<String> inputPort = operator.getInputPortList();
			HashMap<String, DStreamWithSchema> inputDStreamMap = new HashMap<String, DStreamWithSchema>();
			for (String port : inputPort) {
				inputDStreamMap.put(port, RESULT_MAP.get(port));
			}

			// 反射机制
			try {
				Class<?> ownerClass = Class.forName(operator
						.getOpMainClassName());
				Class[] argsClass = { JavaStreamingContext.class, String.class,
						HashMap.class };
				Object[] argsArr = { jssc, arguments, inputDStreamMap };
				Method method = ownerClass.getDeclaredMethod("execute",
						argsClass);
				HashMap<String, DStreamWithSchema> opOutputs = new HashMap<String, DStreamWithSchema>();

				System.out.println(operator.getOpClassName()
						+ " argsXML  >>>>>>>>>>>>>>>>>>>\n"
						+ operator.getArgsXML());

				System.out.print(operator.getOpClassName()
						+ " record >>>>>>> size:" + inputDStreamMap.size()
						+ "record list:\n");
				for (Entry<String, DStreamWithSchema> kv : inputDStreamMap
						.entrySet()) {
					System.out.println(">>:" + kv.getKey());
				}
				opOutputs = (HashMap<String, DStreamWithSchema>) method.invoke(
						ownerClass.newInstance(), argsArr);
				if (opOutputs != null) {
					for (Entry outputi : opOutputs.entrySet()) {
						RESULT_MAP.put((String) outputi.getKey(),
								(DStreamWithSchema) outputi.getValue());
					}
				}

				START_LOG += operator.getOperatorName() + " run success.\n";
			} catch (ClassNotFoundException | NoSuchMethodException
					| SecurityException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| InstantiationException e) {
				e.printStackTrace();
				START_LOG += operator.getOperatorName() + " run failed.\n";
				break;
			} finally {
				System.out.println(outputMessage);
			}
		}
	}

	public static void writeToHDFSFile(String fileName,
			Map<String, String> listOut) {

		String startLog = null;
		startLog = listOut.get("startLog").toString();

		Document document = DocumentHelper.createDocument();
		Element response = document.addElement("log");
		Element startLogNode = response.addElement("startLog");
		startLogNode.setText(startLog);

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
			OutputStream out = fs.create(new Path(fileName),
					new Progressable() {
						public void progress() {
						}
					});
			OutputFormat format = OutputFormat.createPrettyPrint();
			format.setEncoding("UTF-8");
			XMLWriter xmlWriter = new XMLWriter(out, format);
			xmlWriter.write(document);
			xmlWriter.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public static HashMap<String, String> initMainClassMap(
			HashMap<String, String> opClassNameMap, Document document) {
		HashMap<String, String> mainClassMap = new HashMap<String, String>();

		List<Element> operatorElem = document
				.selectNodes("/requestParams/operator/operator");
		for (Element elem : operatorElem) {
			if (opClassNameMap.containsKey(elem.attributeValue("name"))) {
				mainClassMap.put(elem.attributeValue("name"),
						elem.attributeValue("mainClass"));
			}
		}

		return mainClassMap;
	}

	public static List<OpNode> topologicalOrder(
			HashMap<String, OpNode> opGraphMap) {
		List<OpNode> topologicalOrder = new ArrayList<OpNode>();
		Queue<OpNode> queue = new LinkedList<OpNode>();
		while (opGraphMap.size() > 0) {
			for (Entry kv : opGraphMap.entrySet()) {
				if (((OpNode) kv.getValue()).getInDegree() == 0) {
					queue.add(opGraphMap.get(kv.getKey()));
				}
			}
			if (queue.isEmpty() && opGraphMap.size() > 0) {
				System.out.println("This is a loop process,please check it!");
				break;
			}
			while (!queue.isEmpty()) {
				OpNode opNode = queue.poll();
				opGraphMap.remove(opNode.getOperatorName());
				topologicalOrder.add(opNode);
				for (String opChildName : opNode.getChildrenNameList()) {
					OpNode node = opGraphMap.get(opChildName);
					node.setInDegree(node.getInDegree() - 1);
					opGraphMap.put(opChildName, node);
				}
			}
		}
		return topologicalOrder;
	}

	/** 解析过程定义，返回用map存储的图结构。kv对为：<算子名，算子信息节点> */
	public static HashMap<String, OpNode> parsingOpFlow(String hdfsPath) {
		// File file = new File(argsPath);
		HashMap<String, OpNode> opGraphMap = new HashMap<String, OpNode>();
		Document document = readXMLFromHDFS(hdfsPath);
		// 生成算子关系图，初始化算子的opName/inDegree/childrenNameList/inputPortList/opClassName
		opGraphMap = generateOpGraph(document);
		return opGraphMap;
	}

	/** 解析过程定义，返回用map存储的图结构。kv对为：<算子名，算子信息节点> */
	public static Document readXMLFromHDFS(String hdfsPath) {
		HashMap<String, OpNode> opGraphMap = new HashMap<String, OpNode>();
		// 创建saxReader对象
		SAXReader reader = new SAXReader();
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(new Configuration());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Path hdfspath = new Path(hdfsPath);
		FSDataInputStream fsin = null;
		try {
			fsin = fileSystem.open(hdfspath);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Document document = null;
		try {
			document = reader.read(new InputStreamReader(fsin));
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return document;
	}

	/**
	 * 获得hdfs文件内容信息
	 * 
	 * @param hdfsPath
	 *            文件hdfs路径
	 * @return 返回String格式信息
	 * @throws Exception
	 */
	public static String getStringFromHdfs(String hdfsPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path hdfspath = new Path(hdfsPath);
		FSDataInputStream fsin = fileSystem.open(hdfspath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsin));
		String data = "";
		String dataTemp = "";
		while ((dataTemp = reader.readLine()) != null) {
			data += "\n" + dataTemp;
		}
		data = data.substring(1);
		reader.close();
		fsin.close();
		return data;
	}

	public static HashMap<String, OpNode> generateOpGraph(Document document) {
		// 获取所有connect节点信息
		List<Element> connectElem = document
				.selectNodes("/requestParams/operator/connect");
		// 便利过程定义xml中的connect标签内容，过滤掉端口信息，自保留算子名信息的连接关系
		HashSet<String> connectSet = filterConnectSet(connectElem);

		// 获取算子名集合
		HashSet<String> opNameSet = filterOpNameSet(connectSet);

		// 初始化每个算子的孩子列表
		HashMap<String, List<String>> opNameChildMap = initChildMap(opNameSet,
				connectSet);
		// 初始化每个算子的入度
		HashMap<String, Integer> opNameIndegreeMap = initIndegreeMap(opNameSet,
				connectSet);
		// 初始化算子的inputPortList
		HashMap<String, List<String>> opNameInputMap = initIputPortMap(
				opNameSet, connectElem);
		// 初始化算子的xml参数
		HashMap<String, String> opArgsMap = initArgsMap(opNameSet, document);

		// 初始化算子的ClassName
		HashMap<String, String> opClassNameMap = initClassNameMap(opNameSet,
				document);
		// 初始化算子的MainClassName
		HashMap<String, String> opMainClassMap = initMainClassMap(
				opClassNameMap, document);

		// 初始化算子流程图hashmap存储，kv对为<算子名，算子实例>
		HashMap<String, OpNode> opGraphMap = new HashMap<String, OpNode>();
		for (String opName : opNameSet) {
			OpNode opNode = new OpNode();
			opNode.setOperatorName(opName);
			opNode.setInDegree(opNameIndegreeMap.get(opName));
			opNode.setChildrenNameList(opNameChildMap.get(opName));
			opNode.setInputPortList(opNameInputMap.get(opName));
			opNode.setArgsXML(opArgsMap.get(opName));
			opNode.setOpClassName(opClassNameMap.get(opName));
			opNode.setOpMainClassName(opMainClassMap.get(opName));
			opGraphMap.put(opName, opNode);
		}
		return opGraphMap;
	}

	public static HashMap<String, String> initClassNameMap(HashSet opNameSet,
			Document document) {
		HashMap<String, String> opClassNameMap = new HashMap<String, String>();
		List<Element> operatorElem = document
				.selectNodes("/requestParams/operator/operator");
		for (Element elem : operatorElem) {
			if (opNameSet.contains(elem.attributeValue("name"))) {
				opClassNameMap.put(elem.attributeValue("name"),
						elem.attributeValue("class"));
			}
		}
		return opClassNameMap;
	}

	public static HashSet<String> filterOpNameSet(HashSet<String> connectSet) {
		HashSet<String> opNameSet = new HashSet<String>();
		for (String connect : connectSet) {
			opNameSet.add(connect.split(">")[0]);
			opNameSet.add(connect.split(">")[1]);
		}
		return opNameSet;
	}

	public static HashSet<String> filterConnectSet(List<Element> connectElem) {
		HashSet<String> connectSet = new HashSet<String>();
		// 便利过程定义xml中的connect标签内容，过滤掉端口信息，只保留算子名信息
		for (Element node : connectElem) {
			connectSet.add(node.attributeValue("from").split("\\.")[0] + ">"
					+ node.attributeValue("to").split("\\.")[0]);
		}
		return connectSet;
	}

	public static HashMap<String, String> initArgsMap(
			HashSet<String> opNameSet, Document document) {
		HashMap<String, String> opArgsMap = new HashMap<String, String>();
		// 获取根节点元素对象
		Element rootNode = document.getRootElement();
		Element containOp = document.getRootElement().element("operator");
		List<Element> propertys = document
				.selectNodes("/requestParams/operator/context/property");
		Attribute tempPathValue = null;
		String oldTempPath = "";
		for (Element property : propertys) {
			if ("tempHdfsBasePath".equals(property.attributeValue("name"))) {
				oldTempPath = property.attributeValue("value");
				if (!"/".equals(oldTempPath.charAt(oldTempPath.length() - 1))) {
					oldTempPath += "/";
				}
				tempPathValue = property.attribute("value");
			}
		}
		if ("".equals(oldTempPath) || tempPathValue == null) {
			System.out
					.println("The args lack of tempHdfsBasePath in <context> tag.");
		}
		Element context = containOp.element("context");
		Element jobinstanceid = rootNode.element("jobinstanceid");
		List<Element> operators = document
				.selectNodes("/requestParams/operator/operator");
		List<Element> connects = document
				.selectNodes("/requestParams/operator/connect");
		HashMap<String, String> connectPairs = new HashMap<String, String>();
		for (Element connect : connects) {
			connectPairs.put(connect.attributeValue("to"),
					connect.attributeValue("from"));
		}
		for (Element elem : operators) {
			tempPathValue.setValue(oldTempPath + elem.attributeValue("name"));
			String tmpDatasets = "";
			for (Entry<String, String> toFromKV : connectPairs.entrySet()) {
				if (toFromKV.getKey().toString().split("\\.")[0].equals(elem
						.attributeValue("name"))) {

					tmpDatasets += "\t\t<dataset name=\""
							+ toFromKV.getKey().split("\\.")[1]
							+ "\">\n\t\t\t<row>"
							+ toFromKV.getValue().toString()
							+ "</row>\n\t\t</dataset>\n";
				}
			}
			tmpDatasets = "\n\t<datasets>\n" + tmpDatasets + "\t</datasets>";

			String opArg = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<request>\n\t"
					+ jobinstanceid.asXML()
					+ "\n\t"
					+ context.asXML()
					+ "\n\t"
					+ elem.asXML() + tmpDatasets + "\n</request>";

			if (opNameSet.contains(elem.attributeValue("name")))
				opArgsMap.put(elem.attributeValue("name"), opArg);
		}
		return opArgsMap;
	}

	public static HashMap<String, Integer> initIndegreeMap(
			HashSet<String> opNameSet, HashSet<String> connectSet) {
		HashMap<String, Integer> opNameIndegreeMap = new HashMap<String, Integer>();
		for (String opName : opNameSet) {// �遍历算子名集合，构建算有算子的name-childNameList对
			opNameIndegreeMap.put(opName, 0);// 每个节点的入度，将在设置入度时用到
		}
		// 生成孩子列表
		for (String connect : connectSet) {
			opNameIndegreeMap.put(connect.split(">")[1],
					opNameIndegreeMap.get(connect.split(">")[1]) + 1);
		}
		return opNameIndegreeMap;
	}

	public static HashMap<String, List<String>> initChildMap(
			HashSet<String> opNameSet, HashSet<String> connectSet) {
		HashMap<String, List<String>> opNameChildMap = new HashMap<String, List<String>>();
		HashMap<String, Integer> opNameIndegreeMap = new HashMap<String, Integer>();
		for (String opName : opNameSet) {
			opNameIndegreeMap.put(opName, 0);
			opNameChildMap.put(opName, new ArrayList<String>());
			for (String connect : connectSet) {
				if (opName.equals(connect.split(">")[0])) {
					opNameChildMap.get(opName).add(connect.split(">")[1]);
				}
			}
		}
		return opNameChildMap;
	}

	public static HashMap<String, List<String>> initIputPortMap(
			HashSet<String> opNameSet, List<Element> connectElem) {
		HashMap<String, List<String>> opNameInputMap = new HashMap<String, List<String>>();
		for (String opName : opNameSet) {// ����������ϣ������������ӵ�
											// name-childNameList��
			opNameInputMap.put(opName, new ArrayList<String>());
			for (Element node : connectElem) {
				if (opName.equals(node.attributeValue("to").split("\\.")[0])) {
					opNameInputMap.get(opName).add(node.attributeValue("from"));
				}
			}
		}
		return opNameInputMap;
	}

	static class OpNode {
		String operatorName;
		String opClassName;
		String opMainClassName;
		String argsXML;
		int inDegree;
		List<String> inputPortList;
		List<String> childrenNameList;

		public String getOperatorName() {
			return operatorName;
		}

		public void setOperatorName(String operatorName) {
			this.operatorName = operatorName;
		}

		public String getArgsXML() {
			return argsXML;
		}

		public void setArgsXML(String argsXML) {
			this.argsXML = argsXML;
		}

		public int getInDegree() {
			return inDegree;
		}

		public void setInDegree(int inDegree) {
			this.inDegree = inDegree;
		}

		public void decrementInDegree() {
			this.inDegree = this.inDegree--;
		}

		public List<String> getChildrenNameList() {
			return childrenNameList;
		}

		public void setChildrenNameList(List<String> childrenNameList) {
			this.childrenNameList = childrenNameList;
		}

		public String getOpClassName() {
			return opClassName;
		}

		public void setOpClassName(String opClassName) {
			this.opClassName = opClassName;
		}

		public List<String> getInputPortList() {
			return inputPortList;
		}

		public void setInputPortList(List<String> inputPortList) {
			this.inputPortList = inputPortList;
		}

		public String getOpMainClassName() {
			return opMainClassName;
		}

		public void setOpMainClassName(String opMainClassName) {
			this.opMainClassName = opMainClassName;
		}

	}
}
