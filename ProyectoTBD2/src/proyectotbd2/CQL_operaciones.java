package proyectotbd2;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 *
 * @author HP
 */
public class CQL_operaciones {
    
    private static Cluster cluster;
    private static Session session;
    
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
        cquery += "PRIMARY KEY (" + nombreColumnas[primaryKey]+ ")";
        session.execute("CREATE TABLE " + nombre + "(" + cquery +")");
    }
}
