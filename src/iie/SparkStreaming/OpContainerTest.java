package iie.SparkStreaming;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class OpContainerTest {
	public static void main(String[] args) throws Exception {

		/*
		 * JavaStreamingContext jssc = new JavaStreamingContext( new
		 * SparkConf().setAppName("SparkStreamingOperatorTest"), new
		 * Duration(10000));
		 * 
		 * jssc.start(); jssc.awaitTermination();
		 * 
		 * spark-submit --class iie.SparkStreaming.OpContainerTest --master
		 * local /home/dingyu/test.jar --driver-class-path
		 * /home/dingyu/mysql-connector
		 * -java-5.1.37/mysql-connector-java-5.1.37-bin.jar
		 */

		SparkConf sparkConf = new SparkConf().setAppName("SparkTest");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		String xmlPath = "/home/dingyu/xml/procedureDefinition.xml";
		// 解析算子连接关系图，使用map存储
		HashMap<String, OpNode> opGraphMap = parsingOpFlow(xmlPath);

		// 拓扑排序
		List<OpNode> topologicalOrder = topologicalOrder(opGraphMap);

		Map<String, String> resulMap = new HashMap<String, String>();

		// 反射机制拓扑顺序执行算子
		executeOperator(topologicalOrder);

		ctx.stop();
	}

	public static void executeOperator(List<OpNode> topologicalOrder) {
		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (OpNode operator : topologicalOrder) {

			// 获取算子jar包和主类名
			String className = operator.getOpMainClassName();

			String ssc = "";
			String arguments = operator.getArgsXML();
			List<String> inputPort = operator.getInputPortList();
			Map<String, String> inputDStreamMap = new HashMap<String, String>();
			for (String port : inputPort) {
				inputDStreamMap.put(port, resultMap.get(port));
			}

			// 反射机制
			try {
				Class ownerClass = Class.forName("iie.SparkStreaming."
						+ className);
				Class[] argsClass = { ssc.getClass(), arguments.getClass(),
						inputDStreamMap.getClass() };
				Object[] argsArr = { ssc, arguments, inputDStreamMap };
				Method method = ownerClass.getMethod("execute", argsClass);
				HashMap<String, String> opOutputs = new HashMap<String, String>();
				opOutputs = (HashMap<String, String>) method.invoke(
						ownerClass.newInstance(), argsArr);
				for (Entry outputi : opOutputs.entrySet()) {
					resultMap.put((String) outputi.getKey(),
							(String) outputi.getValue());
				}
			} catch (ClassNotFoundException | NoSuchMethodException
					| SecurityException | IllegalAccessException
					| IllegalArgumentException | InvocationTargetException
					| InstantiationException e) {
				e.printStackTrace();
			}
		}
	}

	public static HashMap<String, String> initMainClassMap(
			HashMap<String, String> opClassNameMap) {
		HashMap<String, String> mainClassMap = new HashMap<String, String>();
		try {
			for (Entry kv : opClassNameMap.entrySet()) {
				String sql = "select mainClass from operatorInfo where operatorName = '"
						+ kv.getValue() + "'";// SQL语句
				DBHelper db = new DBHelper(sql);// 创建DBHelper对象
				ResultSet ret = db.pst.executeQuery();// 执行语句，得到结果集
				while (ret.next()) {
					mainClassMap.put(kv.getKey() + "", ret.getString(1));
				}// 显示数据
				if (mainClassMap.size() <= 0)
					System.out.println("wrong operator Class name of "
							+ kv.getValue() + " in args xml");
				db.close();// 关闭连接
			}
		} catch (SQLException e) {
			e.printStackTrace();
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

	/** 解析过程定义文件，返回用map存储的图结构 。kv对为：<算子名，算子信息节点> */
	public static HashMap<String, OpNode> parsingOpFlow(String argsPath) {
		File file = new File(argsPath);
		HashMap<String, OpNode> opGraphMap = new HashMap<String, OpNode>();
		// 创建saxReader对象
		SAXReader reader = new SAXReader();
		// 通过read方法读取一个文件 转换成Document对象
		Document document;
		try {
			document = reader.read(file);
			// 生成算子关系图，初始化opName/inDegree/childrenNameList/inputPortList/opClassName
			opGraphMap = generateOpGraph(document);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		return opGraphMap;
	}

	public static HashMap<String, OpNode> generateOpGraph(Document document) {
		// 获取所有connect节点信息
		List<Element> connectElem = document.selectNodes("/operator/connect");
		// 遍历过程定义xml中的connect标签内容，过滤掉端口信息，只保留算子名信息的连接关系
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
		HashMap<String, String> opMainClassMap = initMainClassMap(opClassNameMap);

		// 初始化算子流程图，使用hashmap存储，kv对为<算子名，算子实例>
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
		List<Element> operatorElem = document.selectNodes("/operator/operator");
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
		// 遍历过程定义xml中的connect标签内容，过滤掉端口信息，只保留算子名信息
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

		// 获取tempHdfsBasePath，并在该路径下增加一层路径，在这里之获取tempHdfsBasePath当前路径属性value值，在每个算子的参数那才修改
		List<Element> propertys = document
				.selectNodes("/operator/context/property");
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
		// 获取context节点所有文字内容
		Element context = rootNode.element("context");
		List<Element> operators = document.selectNodes("/operator/operator");
		List<Element> connects = document.selectNodes("/operator/connect");
		// 保存算子端口连接信息，key是to 即输入端口，value是from 即输出端口。
		HashMap<String, String> connectPairs = new HashMap<String, String>();
		for (Element connect : connects) {
			connectPairs.put(connect.attributeValue("to"),
					connect.attributeValue("from"));
		}
		for (Element elem : operators) {
			// 修改tempHdfsBasePath的value属性值
			tempPathValue.setValue(oldTempPath + elem.attributeValue("name"));
			String tmpDatasets = "";// datasets内容
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
					+ context.asXML()
					+ "\n\t"
					+ elem.asXML()
					+ tmpDatasets
					+ "\n</request>";

			if (opNameSet.contains(elem.attributeValue("name")))
				opArgsMap.put(elem.attributeValue("name"), opArg);
		}
		return opArgsMap;
	}

	public static HashMap<String, Integer> initIndegreeMap(
			HashSet<String> opNameSet, HashSet<String> connectSet) {
		HashMap<String, Integer> opNameIndegreeMap = new HashMap<String, Integer>();
		for (String opName : opNameSet) {// 遍历算子名集合，构建所有算子的 name-childNameList对
			opNameIndegreeMap.put(opName, 0);// 每个节点的入度，将在设置入度时用到
		}
		// 初始化每个节点的入度
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
		for (String opName : opNameSet) {// 遍历算子名集合，构建所有算子的 name-childNameList对
			opNameIndegreeMap.put(opName, 0);// 每个节点的入度，将在设置入度时用到
			opNameChildMap.put(opName, new ArrayList<String>());
			for (String connect : connectSet) {
				// 生成孩子列表
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
		for (String opName : opNameSet) {// 遍历算子名集合，构建所有算子的 name-childNameList对
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
