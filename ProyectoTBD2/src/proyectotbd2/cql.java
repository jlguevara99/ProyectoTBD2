package proyectotbd2;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import java.util.ArrayList;
import jdk.nashorn.internal.runtime.JSType;

/**
 *
 * @author HP
 */
public class cql {
    
    public static Cluster cluster;
    public static Session session;
    
    public static Cluster connect(String node){
        return Cluster.builder().addContactPoint(node).build();
    }
    
    public static void IniciarConexion(){
        try {
            cluster = connect("127.0.0.1");
        } catch (NoHostAvailableException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void IniciarSesion (String KeySpaceName){
        try {
            session = cluster.connect(KeySpaceName);
        } catch (NoHostAvailableException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void Crear_KeySpace (String KeySpaceName){
        session = cluster.connect();
        session.execute("CREATE KEYSPACE "+ KeySpaceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
    }
    
    public static void Eliminar_keySpace (String KeySpaceName){
        session.execute("DROP KEYSPACE "+ KeySpaceName);
    }
    
    public static void finalizarConexion (){
        session.close();
        cluster.close();
    }
    
    public static void crear_tabla (String nombre, String[] nombreColumnas, String[] tipoDato, int primaryKey){
        String cquery = "";
        for (int i = 0; i < nombreColumnas.length; i++) {
            cquery += nombreColumnas[i] + " " + tipoDato[i] +",";
        }
        cquery += "PRIMARY KEY(" + nombreColumnas[primaryKey]+ ")";
        System.out.println(cquery);
        session.execute("CREATE TABLE " + nombre + "(" + cquery +")");
    }
    
    public static void eliminar_t(String nombre){
        session.execute("DROP TABLE " + nombre);
    }
    
    public static void insertar(String nombre, String[] columnas, ArrayList valores){
        String campos = "";
        String values = "";
        
        for (int i = 0; i < columnas.length; i++) {
            campos += columnas[i] + ",";
            if (!(valores.get(i) instanceof Integer)) {
                values += "'" + valores.get(i) + "'" + ",";
            }else{
                values += valores.get(i) + ",";
            }
        }
        campos = campos.substring(0, campos.length()-1);
        values = values.substring(0, values.length()-1);
        System.out.println("INSERT INTO " + nombre + " (" + campos + ") VALUES (" + values + ")");
        session.execute("INSERT INTO " + nombre + " (" + campos + ") VALUES (" + values + ")");
    }
    
}
